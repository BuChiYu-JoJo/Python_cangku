import requests
import concurrent.futures
import csv
import time
import os
import threading
import random
import queue
import signal
import sys
from urllib.parse import quote
from collections import deque
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor

# ================== 全局配置 ==================
CONFIG = {
    "DURATION": 60,        # 测试总时长（秒）
    "CONCURRENCY": 100,              # 并发线程数
    "MIN_CONTENT": 15 * 1024,        # 有效内容阈值（15KB）
    "STATS_INTERVAL": 5,             # 监控刷新间隔
    "MAX_TIMEOUT": 30,               # 请求超时时间
    "BUFFER_SIZE": 200,              # 数据缓存条数
    "LOG_ROTATE": 60 * 60,           # 错误日志轮转时间（秒）
    "SERVICES": ["brightdata"],      # 要测试的服务列表
    "QUERIES": [
        "pizza", "Apple", "Banana", "Bread", "Cake", "Cookie", "Egg",
        "Fish", "Meat", "Noodle", "Salad", "Soup", "Steak", "Taco",
        "Pasta", "Rice", "Burger", "Sushi", "Sandwich", "Pancake"
    ]
}

# ================== 优雅退出处理 ==================
class GracefulExiter:
    def __init__(self):
        self.state = False
        signal.signal(signal.SIGINT, self.change_state)
        signal.signal(signal.SIGTERM, self.change_state)

    def change_state(self, signum, frame):
        print(f"\n捕获退出信号 {signum}, 开始优雅退出...")
        self.state = True

    def exit(self):
        return self.state

# ================== 实时性能监控 ==================
class PerformanceMonitor:
    def __init__(self, duration):
        self.start_time = time.time()
        self.duration = duration
        self.lock = threading.Lock()
        self.reset_counters()
        
        # 初始化滑动窗口（最近500次请求）
        self.latency_window = deque(maxlen=500)
        self._init_log_rotator()

    def _init_log_rotator(self):
        self.last_rotate = time.time()
        self.error_log = None
        self.rotate_error_log()

    def rotate_error_log(self):
        if self.error_log:
            self.error_log.close()
        log_path = f"errors_{time.strftime('%Y%m%d%H%M%S')}.log"
        self.error_log = open(log_path, 'a', buffering=1)
        self.last_rotate = time.time()

    def reset_counters(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.total_latency = 0.0
        self.success_latency = 0.0

    def record(self, success, latency):
        with self.lock:
            self.total += 1
            if success:
                self.success += 1
                self.success_latency += latency
            else:
                self.failed += 1
            self.total_latency += latency
            self.latency_window.append(latency)

    def log_error(self, service, req_id, query, error, status_code=None):
        if time.time() - self.last_rotate > CONFIG["LOG_ROTATE"]:
            self.rotate_error_log()
        
        log_entry = (
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"Service: {service} | ReqID: {req_id} | Query: {query} | "
            f"Status: {status_code or 'N/A'} | Error: {error}\n"
        )
        self.error_log.write(log_entry)

    def get_stats(self):
        window_avg = sum(self.latency_window)/len(self.latency_window) if self.latency_window else 0
        success_avg = self.success_latency/self.success if self.success else 0
        return {
            "elapsed": time.time() - self.start_time,
            "remaining": max(0, self.duration - (time.time() - self.start_time)),
            "total": self.total,
            "success": self.success,
            "failed": self.failed,
            "success_rate": self.success/self.total*100 if self.total else 0,
            "window_avg": window_avg,
            "success_avg": success_avg
        }

    def close(self):
        if self.error_log:
            self.error_log.close()

# ================== 数据记录器 ==================
class DataRecorder:
    def __init__(self, service_name):
        self.service = service_name
        self.buffer = []
        self.active = True
        os.makedirs(service_name, exist_ok=True)
        self._init_writers()
        self._start_flush_thread()

    def _init_writers(self):
        self.csv_file = open(f'{self.service}/requests.csv', 'w', buffering=1)
        self.writer = csv.writer(self.csv_file)
        self.writer.writerow([
            'Timestamp', 'RequestID', 'Query', 'Status', 
            'Latency(ms)', 'ContentLength', 'Error'
        ])

    def _start_flush_thread(self):
        def flush_daemon():
            while self.active:
                time.sleep(1)
                self.flush()
        threading.Thread(target=flush_daemon, daemon=True).start()

    def log(self, req_id, query, success, latency, length, error=""):
        entry = (
            time.strftime('%Y-%m-%d %H:%M:%S'),
            req_id,
            query,
            'Success' if success else 'Failed',
            f"{latency:.2f}",
            length,
            error
        )
        self.buffer.append(entry)
        if len(self.buffer) >= CONFIG["BUFFER_SIZE"]:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        with self.csv_file:
            self.writer.writerows(self.buffer)
        self.buffer.clear()

    def close(self, stats):
        self.active = False
        self.flush()
        with open(f'{self.service}/report.txt', 'w') as f:
            f.write(f"总请求数: {stats['total']}\n")
            f.write(f"成功率: {stats['success_rate']:.2f}%\n")
            f.write(f"平均延迟: {stats['success_avg']:.2f}ms\n")
            f.write(f"窗口平均延迟: {stats['window_avg']:.2f}ms\n")
        self.csv_file.close()

# ================== 请求处理器 ==================
class RequestEngine:
    def __init__(self, service, monitor, recorder):
        self.service = service
        self.monitor = monitor
        self.recorder = recorder
        self.req_counter = 1
        self.session_pool = ThreadPoolExecutor(max_workers=CONFIG["CONCURRENCY"])
        self.session_map = {}
        self._init_sessions()

    def _init_sessions(self):
        for _ in range(CONFIG["CONCURRENCY"]):
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=20, pool_maxsize=100)
            session.mount('https://', adapter)
            self.session_map[session] = True

    def _get_session(self):
        while True:
            for session, available in self.session_map.items():
                if available:
                    self.session_map[session] = False
                    return session
            time.sleep(0.1)

    def _release_session(self, session):
        self.session_map[session] = True

    def execute(self):
        query = random.choice(CONFIG["QUERIES"])
        req_id = self.req_counter
        self.req_counter += 1

        session = self._get_session()
        try:
            start_time = time.time()
            success, latency, length, error = self._make_request(session, query)
            self.monitor.record(success, latency)
            self.recorder.log(req_id, query, success, latency, length, error)
            if not success:
                self.monitor.log_error(self.service, req_id, query, error)
        finally:
            self._release_session(session)

    def _make_request(self, session, query):
        url = self._build_url(query)
        headers = self._get_headers()
        error = ""
        status_code = None
        content_length = 0
        start_time = time.time()

        try:
            # 构造POST请求的payload
            payload = {
                "zone": "serp_api1",
                "url": f"https://www.bing.com/search?q={query}",
                "format": "json"
            }

            # 改为POST请求并发送JSON数据
            response = session.post(
                url,
                headers=headers,
                json=payload,  # 使用json参数自动设置Content-Type
                timeout=CONFIG["MAX_TIMEOUT"]
            )

            status_code = response.status_code
            content_length = len(response.content)

            if response.status_code == 200:
                if content_length >= CONFIG["MIN_CONTENT"]:
                    success = True
                else:
                    success = False
                    error = f"内容不足 {content_length}/{CONFIG['MIN_CONTENT']}"
            else:
                success = False
                error = f"HTTP {status_code}"
        except Exception as e:
            success = False
            error = f"{type(e).__name__}: {str(e)}"
        finally:
            latency = (time.time() - start_time) * 1000

        return success, latency, content_length, error

    def _build_url(self, query):
        if self.service == "brightdata":
            return "https://api.brightdata.com/request"  # 移除URL参数
        else:
            return (
                "http://170.106.180.18:9004/spiders?"
                f"search_url={quote(f'https://www.bing.com/search?q={query}')}"
            )

    def _get_headers(self):
        if self.service == "brightdata":
            return {
                "Authorization": "Bearer 0589abd75f5c73032acb47491ac0693f59ca11ae9dfd6fc36b4390b9ca3109be",
                "Content-Type": "application/json"
            }
        return {}

# ================== 服务运行器 ==================
def run_service(service_name, exiter):
    print(f"🚀 启动 {service_name} 压力测试...")
    monitor = PerformanceMonitor(CONFIG["DURATION"])
    recorder = DataRecorder(service_name)
    engine = RequestEngine(service_name, monitor, recorder)
    
    # 状态显示线程
    def display_stats():
        while not exiter.exit():
            stats = monitor.get_stats()
            os.system('cls' if os.name == 'nt' else 'clear')
            print(f"""
            ==== {service_name.upper()} 测试状态 ====
            已运行: {stats['elapsed']:.1f}s / 剩余: {stats['remaining']:.1f}s
            总请求: {stats['total']} | 成功: {stats['success']} | 失败: {stats['failed']}
            成功率: {stats['success_rate']:.2f}% | 实时延迟: {stats['window_avg']:.2f}ms
            平均成功延迟: {stats['success_avg']:.2f}ms
            """)
            time.sleep(CONFIG["STATS_INTERVAL"])
    threading.Thread(target=display_stats, daemon=True).start()

    # 主请求循环
    end_time = time.time() + CONFIG["DURATION"]
    with ThreadPoolExecutor(max_workers=CONFIG["CONCURRENCY"]) as executor:
        futures = []
        while time.time() < end_time and not exiter.exit():
            futures.append(executor.submit(engine.execute))
            # 清理已完成的future对象
            futures = [f for f in futures if not f.done()]
            time.sleep(0.001)  # 防止CPU空转

    # 收尾工作
    final_stats = monitor.get_stats()
    recorder.close(final_stats)
    monitor.close()
    print(f"✅ {service_name} 测试完成! 报告已保存至 {service_name}/ 目录")

# ================== 主程序 ==================
if __name__ == "__main__":
    exiter = GracefulExiter()
    print("""
    ╔══════════════════════════════╗
    ║      高级压力测试系统       ║
    ╚══════════════════════════════╝
    """)
    
    for service in CONFIG["SERVICES"]:
        if exiter.exit():
            break
        run_service(service, exiter)
    
    print("\n🏁 所有测试任务完成！")