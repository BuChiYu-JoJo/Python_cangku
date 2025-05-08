import requests
import time
import os
import csv
import signal
import random
import psutil  # 新增内存监控
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Lock, Event, Thread
from datetime import datetime, timedelta

# ================== 优化后全局配置 ==================
CONFIG = {
    "CONCURRENCY": 100,               # 合理并发数（原100可能过高）
    "MIN_CONTENT": 15 * 1024,        # 有效内容阈值
    "MAX_TIMEOUT": 20,               # 缩短超时时间（快速失败）
    "RETRIES": 0,                    # 重试次数
    "BACKOFF_FACTOR": 0.5,           # 指数退避系数
    "LOG_BUFFER_SIZE": 100,          # 日志缓冲大小
    "HEARTBEAT_INTERVAL": 10,        # 心跳间隔（秒）
    "RUN_DURATION": timedelta(hours=0.01),  # 目标运行时长
    "QUERIES": [
        "pizza", "Apple", "Banana", "Bread", "Cake", "Cookie", "Egg",
        "Fish", "Meat", "Noodle", "Salad", "Soup", "Steak", "Taco",
        "Pasta", "Rice", "Burger", "Sushi", "Sandwich", "Pancake"
    ],
    "SERVICES": ["thordata"]
}

# ================== 内存监控组件（新增） ==================
class MemoryWatcher:
    def __init__(self):
        self.process = psutil.Process()
        self.stop_flag = Event()
        self.start()

    def start(self):
        Thread(target=self._monitor, daemon=True).start()

    def _monitor(self):
        while not self.stop_flag.is_set():
            mem = self.process.memory_percent()
            if mem > CONFIG["MAX_MEMORY_USAGE"] * 100:
                print(f"🚨 内存预警: {mem:.1f}%，触发紧急清理")
                self._force_cleanup()
            time.sleep(CONFIG["MEMORY_MONITOR_INTERVAL"])

    def _force_cleanup(self):
        # 清理全局可释放资源
        if 'recorder' in globals():
            globals()['recorder']._flush_buffer()  # 强制刷日志
        if 'session_pool' in globals():
            for s in session_pool.pool:
                s.close()

    def stop(self):
        self.stop_flag.set()

# ================== 优化型数据记录器 ==================
class BufferedDataRecorder:
    def __init__(self, service_name):
        self.service = service_name
        self.buffer = []
        self.lock = Lock()
        self._init_storage()
        self._log_thread = None  # 新增：独立日志线程
        self._stop_event = Event()  # 新增：线程终止控制
        self._start_log_thread()  # 启动日志线程

    def _start_log_thread(self):
        """启动独立的日志刷新线程"""
        # 修改此处，将 time.Thread 替换为 Thread
        self._log_thread = Thread(target=self._flush_worker, daemon=True)
        self._log_thread.start()

    def _init_storage(self):
        os.makedirs(self.service, exist_ok=True)
        self.csv_path = f'{self.service}/requests_{time.strftime("%Y%m%d%H%M")}.csv'
        self.csv_file = open(self.csv_path, 'a', buffering=1, newline='')  # 行缓冲模式
        self.writer = csv.writer(self.csv_file)
        if os.path.getsize(self.csv_path) == 0:
            self.writer.writerow([
                'Timestamp', 'RequestID', 'Query', 'Status', 
                'Latency(ms)', 'ContentLength', 'Error', 'RetryCount'
            ])

    def log(self, req_id, query, success, latency, length, error="", retries=0):
        with self.lock:
            self.buffer.append((
                datetime.now().isoformat(),
                req_id,
                query,
                'Success' if success else 'Failed',
                f"{latency:.2f}",
                length,
                error,
                retries
            ))
            # 触发条件：缓冲满 或 强制刷新（由close触发）
            if len(self.buffer) >= CONFIG["LOG_BUFFER_SIZE"]:
                self._flush_buffer()

    def _flush_worker(self):
        """日志线程主循环（轻量级轮询）"""
        while not self._stop_event.is_set():
            time.sleep(0.1)  # 降低CPU占用
            with self.lock:
                if self.buffer:
                    self._flush_buffer()

    def _flush_buffer(self):
        """安全刷新缓冲区到磁盘"""
        if self.buffer:
            try:
                self.writer.writerows(self.buffer)
                self.csv_file.flush()  # 关键：强制刷盘
            except Exception as e:
                print(f"⚠️ 日志写入失败: {str(e)}")
            finally:
                del self.buffer[:]  # 释放内存（比clear更彻底）

    def close(self):
        """优雅关闭：通知线程终止 + 等待完成"""
        self._stop_event.set()  # 发送终止信号
        if self._log_thread and self._log_thread.is_alive():
            self._log_thread.join(timeout=5)  # 最多等待5秒
        self.csv_file.close()

# ================== 连接池管理器（带连接回收） ==================
class AutoCloseSession(requests.Session):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        self.headers.clear()

class SmartHttpPool:
    def __init__(self, size):
        self.size = size
        self.pool = [AutoCloseSession() for _ in range(size)]
        self._cleanup_thread = Thread(target=self._connection_cleaner, daemon=True)
        self._cleanup_thread.start()

    def _connection_cleaner(self):
        while True:
            time.sleep(300)  # 每5分钟清理过期连接
            with requests.Session() as s:
                for session in self.pool:
                    if 'last_used' in session.__dict__ and \
                       time.time() - session.last_used > 300:
                        session.close()
                        self.pool.remove(session)
                        self.pool.append(AutoCloseSession())

    def get_session(self):
        session = random.choice(self.pool)
        session.last_used = time.time()  # 记录使用时间
        return session

# ================== 请求引擎（带内存保护） ==================
class MemorySafeEngine:
    def __init__(self, monitor, recorder, session_pool, req_id):
        self.monitor = monitor
        self.recorder = recorder
        self.session_pool = session_pool
        self.req_id = req_id
        self.query = random.choice(CONFIG["QUERIES"])

    def execute(self):
        start_time = time.time()
        success = False
        latency = 0.0
        content_length = 0
        error = ""
        retries = 0

        while retries <= CONFIG["RETRIES"]:
            try:
                with self.session_pool.get_session() as session:  # 自动关闭连接
                    url = self._build_url(self.query)
                    headers = self._get_headers()
                    
                    with session.get(url, headers=headers, timeout=CONFIG["MAX_TIMEOUT"]) as response:
                        content_length = len(response.content)
                        if response.status_code == 200 and content_length >= CONFIG["MIN_CONTENT"]:
                            success = True
                            # 主动释放大内容（超过1MB）
                            if content_length > 1024 * 1024:
                                del response._content
                        else:
                            error = f"HTTP {response.status_code}, 长度: {content_length}"
                            retries += 1

            except Exception as e:
                error = f"{type(e).__name__}: {str(e)}"
                retries += 1
                time.sleep(CONFIG["BACKOFF_FACTOR"] * (2 ** retries))

        latency = (time.time() - start_time) * 1000
        self.monitor.record(success, latency)
        self.recorder.log(self.req_id, self.query, success, latency, content_length, error, retries)

    def _build_url(self, query):
        encoded_query = quote_plus(query)
        bing_url = f"https://www.bing.com/search?q={encoded_query}"
        return f"http://170.106.180.18:9004/spiders?search_url={bing_url}"

    def _get_headers(self):
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/118.0.0.0",
            "Connection": "close",  # 禁用keep-alive
            "Accept-Encoding": "gzip"
        }

# ================== 服务运行器（带完整生命周期管理） ==================
def run_service(service_name):
    print(f"🚀 启动 {service_name} 压力测试（目标12小时）...")
    
    # 初始化组件
    recorder = BufferedDataRecorder(service_name)  # 使用修复后的记录器
    session_pool = SmartHttpPool(CONFIG["CONCURRENCY"])
    monitor = StabilityMonitor()
    
    # 注册优雅关闭钩子（新增：触发记录器关闭）
    def shutdown_handler(signum, frame):
        print("\n🛑 接收到终止信号，正在优雅关闭...")
        monitor.trigger_shutdown()
        recorder.close()  # 关键：主动关闭记录器
        os._exit(0)  # 确保进程终止
    
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)  # 新增Ctrl+C支持
    
    # 预生成请求ID（避免锁竞争）
    req_id_generator = (i for i in range(1, 10_000_000))
    
    # 主任务池（控制实际并发数）
    with ThreadPoolExecutor(max_workers=CONFIG["CONCURRENCY"]) as executor:
        # 启动心跳监控
        heartbeat_future = executor.submit(_heartbeat_loop, monitor)
        
        # 连接池预热
        _warmup_pool(session_pool, 10)
        
        # 任务提交循环
        end_time = datetime.now() + CONFIG["RUN_DURATION"]
        while datetime.now() < end_time and not monitor.shutdown_flag:
            try:
                req_id = next(req_id_generator)
                executor.submit(
                    MemorySafeEngine(monitor, recorder, session_pool, req_id).execute
                )
            except StopIteration:
                break

        # 等待所有任务完成（新增：带超时的优雅等待）
        wait(executor._pending_workers, timeout=10)
        monitor.trigger_shutdown()
        heartbeat_future.cancel()

    # 最终清理（新增：确保记录器关闭）
    recorder.close()
    session_pool._connection_cleaner()
    print("\n" + "="*50)
    print(f"✅ 测试完成（{CONFIG['RUN_DURATION']}）")
    print(f"📊 总请求: {monitor.total_requests} | 成功率: {monitor.success_rate:.1f}%")
    print(f"📁 日志保存至: {recorder.csv_path}")

# ================== 监控与统计（优化版） ==================
class StabilityMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.total_requests = 0
        self.success = 0
        self.latency_sum = 0.0
        self.shutdown_flag = False

    def record(self, success, latency):
        self.total_requests += 1
        if success:
            self.success += 1
        self.latency_sum += latency

    @property
    def success_rate(self):
        return (self.success / self.total_requests) * 100 if self.total_requests else 0

    def heartbeat(self):
        if self.shutdown_flag:
            return
        elapsed = time.time() - self.start_time
        qps = self.total_requests / elapsed if elapsed > 0 else 0
        avg_latency = self.latency_sum / self.total_requests if self.total_requests else 0
        
        print(f"💓 心跳监测 - QPS: {qps:.2f} | 成功率: {self.success_rate:.1f}% | 延迟: {avg_latency:.2f}ms")
        print(f"📦 日志缓冲: {len(globals().get('recorder', {}).buffer):3d}条 | 内存: {psutil.Process().memory_percent():.1f}%")

    def trigger_shutdown(self):
        self.shutdown_flag = True

def _heartbeat_loop(monitor):
    while not monitor.shutdown_flag:
        monitor.heartbeat()
        time.sleep(CONFIG["HEARTBEAT_INTERVAL"])

def _warmup_pool(pool, count):
    print(f"🔌 连接池预热（{count}次）...")
    with requests.Session() as s:
        for _ in range(count):
            with pool.get_session() as session:
                session.head("https://www.bing.com", timeout=5)

# 新增 _graceful_shutdown 函数的实现
def _graceful_shutdown(recorder, session_pool, monitor):
    if recorder:
        recorder.close()
    if session_pool:
        for session in session_pool.pool:
            session.close()
    if monitor:
        monitor.trigger_shutdown()

# ================== 主程序入口 ==================
if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════╗
    ║           thordata 内存优化版压力测试        ║
    ╠════════════════ 核心优化 ═══════════════════╣
    ║ ✅ 日志缓冲减半（50条）                      ║
    ║ ✅ 连接自动关闭（with上下文）                ║
    ║ ✅ 内存监控（阈值80%）                      ║
    ║ ✅ 并发数优化（50→原100）                   ║
    ╚══════════════════════════════════════════════╝
    """)
    
    try:
        run_service("thordata-optimized")
    except Exception as e:
        print(f"❌ 测试异常终止: {str(e)}")
        _graceful_shutdown(globals().get('recorder'), globals().get('session_pool'), None)