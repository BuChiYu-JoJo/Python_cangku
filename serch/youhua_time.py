import requests
import concurrent.futures
import csv
import gc
import time
import os
import threading
import random
import queue
from urllib.parse import quote
from collections import deque
from requests.adapters import HTTPAdapter
from threading import BoundedSemaphore

# ================== 全局配置 ==================
DURATION = 0.02 * 60 * 60  # 运行时长（秒）测试用60秒
MIN_CONTENT_LENGTH = 15 * 1024  # 15KB阈值
CONCURRENCY = 50  # 并发线程数
MAX_REQUEST_TIMEOUT = 3.05 + 30  # 最大请求超时时间
queries = [
    "pizza", "Apple", "Banana", "Bread", "Cake", "Cookie", "Egg", "Fish", "Meat", "Noodle",
    "Salad", "Soup", "Steak", "Taco",
    "Pasta", "Rice", "Burger", "Sushi", "Sandwich",
    "Pancake", "Waffle", "Yogurt", "Cheese", "Ham",
    "Sausage", "Chicken", "Turkey", "Dumpling",
    "Curry", "Tofu", "Omelette", "Pudding", "Pie"
]
STATS_INTERVAL = 1  # 统计显示间隔
SERVICES_TO_RUN = ["thordata"]


# ================== 实时监控模块 ==================
class LiveMonitor:
    def __init__(self, duration):
        self.queue = queue.Queue(maxsize=10000)
        self.start_time = time.time()
        self.duration = duration
        self.total = 0
        self.success = 0
        self.fail = 0
        self.latencies = deque(maxlen=500)
        self.success_latency_sum = 0
        self.success_count = 0
        self.last_total = 0
        self.last_time = self.start_time
        self._running = True
        self.process_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.display_thread = threading.Thread(target=self._display_loop, daemon=True)
        self.process_thread.start()
        self.display_thread.start()

    def _process_queue(self):
        while self._running or not self.queue.empty():
            try:
                item = self.queue.get(timeout=0.5)
                if item['type'] == 'success':
                    self.total += 1
                    self.success += 1
                    self.success_latency_sum += item['latency']
                    self.success_count += 1
                    self.latencies.append(item['latency'])
                elif item['type'] == 'failure':
                    self.total += 1
                    self.fail += 1
                    self.latencies.append(item['latency'])
                self.queue.task_done()
            except queue.Empty:
                if not self._running:
                    break

    def _display_loop(self):
        while self._running:
            time.sleep(STATS_INTERVAL)
            self._print_stats()
        self._print_stats()  # 最终输出

    def _print_stats(self):
        current_time = time.time()
        elapsed = current_time - self.start_time
        remaining = max(0, self.duration - elapsed)
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0

        time_diff = current_time - self.last_time
        current_total = self.total
        current_rps = (current_total - self.last_total) / time_diff if time_diff > 0 else 0
        self.last_total = current_total
        self.last_time = current_time

        avg_success = self.success_latency_sum / self.success_count if self.success_count else 0

        display = [
            "\n" + "=" * 50,
            f"实时监控 @ {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "-" * 50,
            f"运行时间: {self._format_duration(elapsed)} | 剩余时间: {self._format_duration(remaining)}",
            f"总请求量: {self.total} | 成功: {self.success} | 失败: {self.fail}",
            f"成功率: {self.success / self.total * 100:.2f}%" if self.total else "成功率: N/A",
            f"实时QPS: {current_rps:.2f}/s | 平均延迟: {avg_latency:.2f}ms | 平均成功延迟: {avg_success:.2f}ms",
            "=" * 50
        ]
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\n".join(display))

    def _format_duration(self, seconds):
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    def record_success(self, latency):
        self.queue.put({'type': 'success', 'latency': latency}, block=False)

    def record_failure(self, latency):
        self.queue.put({'type': 'failure', 'latency': latency}, block=False)

    def get_final_stats(self):
        if self.total == 0:
            return 0.0, 0.0
        success_rate = (self.success / self.total) * 100
        avg_success = self.success_latency_sum / self.success_count if self.success_count else 0.0
        return avg_success, success_rate

    def stop(self):
        self._running = False
        self.process_thread.join(timeout=2.0)
        self.display_thread.join(timeout=1.0)
        self.queue.join()


# ================== 数据记录模块 ==================
class DataRecorder:
    def __init__(self, service_name):
        self.service = service_name
        os.makedirs(service_name, exist_ok=True)
        self.buffer = []
        self.buffer_size = 200
        self.buffer_lock = threading.Lock()
        self.csv_file = None
        self.closed = False  # 新增关闭标志
        self._init_file()

    def _init_file(self):
        self.csv_file = open(f'{self.service}/requests.csv', 'w', newline='', encoding='utf-8', buffering=2048)
        self.writer = csv.writer(self.csv_file)
        self.writer.writerow([
            'Timestamp', 'RequestID', 'Query',
            'Status', 'Latency(ms)', 'ContentLength', '错误信息', 'AvgSuccessLatency(ms)', '请求成功率'
        ])

    def log_request(self, req_id, query, status, latency, length, error_info=""):
        if self.closed:  # 检查是否已关闭
            return
        with self.buffer_lock:
            self.buffer.append((
                time.strftime('%Y-%m-%d %H:%M:%S'),
                req_id,
                query,
                'Success' if status else 'Failed',
                f"{latency:.2f}",
                length,
                error_info,
                "",
                ""
            ))
            if len(self.buffer) >= self.buffer_size:
                self._flush_buffer()

    def _flush_buffer(self):
        with self.buffer_lock:
            current_buffer = list(self.buffer)
            self.buffer.clear()
        for row in current_buffer:
            self.writer.writerow(row)
        self.csv_file.flush()

    def close(self, avg_success_latency, success_rate):
        self.closed = True  # 标记关闭状态
        self._flush_buffer()
        if not self.csv_file.closed:
            self.csv_file.close()
        # 重新以追加模式打开文件写入统计数据
        with open(f'{self.service}/requests.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["", "", "", "", "", "", f"{avg_success_latency:.2f}", f"{success_rate:.2f}%"])


# ================== 信号量回调处理 ==================
def release_semaphore(future, semaphore):
    try:
        semaphore.release()
    except ValueError:
        pass  # 忽略信号量超过初始值的错误


# ================== 新增全局关闭协调器 ==================
class ShutdownCoordinator:
    def __init__(self):
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._active_connections = 0  # 跟踪进行中的网络连接

    def request_shutdown(self):
        """触发优雅关闭"""
        with self._lock:
            if not self._event.is_set():
                self._event.set()
                print("[SYSTEM] 全局关闭信号已激活")

    def should_stop(self):
        """检查是否应该停止当前操作"""
        return self._event.is_set()

    def connection_enter(self):
        """注册新连接"""
        with self._lock:
            self._active_connections += 1

    def connection_exit(self):
        """注销完成连接"""
        with self._lock:
            self._active_connections -= 1

    def active_connections(self):
        """获取当前活跃连接数"""
        with self._lock:
            return self._active_connections


# ================== 请求处理模块 ==================
def get_session():
    thread_local = threading.local()
    if not hasattr(thread_local, 'session'):
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=100,
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        thread_local.session = session
    return thread_local.session


def request_handler(service_name, req_id, query, monitor, recorder, shutdown_coord):
    """
    改进后的请求处理器，支持：
    - 优雅关闭中断
    - 连接资源跟踪
    - 增强的错误处理
    """
    # 前置关闭检查
    if shutdown_coord.should_stop():
        recorder.log_request(req_id, query, False, 0, 0, "请求被取消(前置检查)")
        return False

    session = None
    response = None
    start_time = time.time()
    latency = 0.0
    content_length = 0
    status_code = 0
    error_type = ""
    error_details = ""
    content_sample = []
    success = False

    try:
        # 注册连接并获取会话
        shutdown_coord.connection_enter()
        session = get_session()

        # 构建请求参数
        if service_name == "brightdata":
            url = "https://api.brightdata.com/request"
            headers = {
                "Authorization": f"Bearer {os.getenv('BRIGHTDATA_API_KEY')}",
                "Content-Type": "application/json"
            }
            payload = {
                "zone": "serp_api1",
                "url": f"https://www.google.com/search?q={query}",
                "format": "json"
            }
            response = session.post(
                url,
                headers=headers,
                json=payload,
                timeout=(3.05, 30),
                stream=True
            )
        else:
            encoded_query = quote(f'https://www.google.com/search?q={query}', safe=':/?=')
            url = f"http://170.106.180.18:9004/spiders?search_url={encoded_query}"
            response = session.get(url, timeout=(3.05, 30), stream=True)

        # 处理响应内容 (支持中断的流式读取)
        with response:
            status_code = response.status_code
            if status_code != 200:
                raise requests.exceptions.HTTPError(f"HTTP {status_code}")

            # 分块读取内容并检查关闭状态
            for chunk in response.iter_content(chunk_size=4096):
                if shutdown_coord.should_stop():
                    raise RuntimeError("主动终止请求流")

                content_length += len(chunk)
                if len(content_sample) < 4:  # 采样前4个块（约16KB）
                    content_sample.append(chunk)

                # 内容长度阈值检查
                if content_length > MIN_CONTENT_LENGTH * 2:  # 提前终止大响应
                    response.close()
                    break

            # 验证内容完整性
            if content_length < MIN_CONTENT_LENGTH:
                raise ValueError(f"内容不足 {content_length}/{MIN_CONTENT_LENGTH} bytes")

            success = True

    except requests.exceptions.Timeout as e:
        error_type = "Timeout"
        error_details = f"{type(e).__name__} {str(e)}"
    except requests.exceptions.HTTPError as e:
        error_type = f"HTTP {status_code}"
        error_details = f"{response.text[:200]}" if response else "无响应内容"
    except requests.exceptions.RequestException as e:
        error_type = "NetworkError"
        error_details = f"{type(e).__name__} {str(e)}"
    except RuntimeError as e:
        if "主动终止请求流" in str(e):
            error_type = "Shutdown"
            error_details = "程序关闭期间终止"
        else:
            error_type = "RuntimeError"
            error_details = str(e)
    except Exception as e:
        error_type = "UnexpectedError"
        error_details = f"{type(e).__name__} {str(e)}"
    finally:
        try:
            # 计算延迟
            latency = (time.time() - start_time) * 1000  # 毫秒

            # 资源清理
            if response:
                response.close()
            if session:
                session.close()
            shutdown_coord.connection_exit()

            # 记录监控数据
            if success:
                monitor.record_success(latency)
            else:
                monitor.record_failure(latency)

            # 构建错误日志
            error_info = f"{error_type}: {error_details}" if error_type else ""
            if not success and not error_info:
                error_info = "未知错误"

            # 记录到CSV
            recorder.log_request(
                req_id=req_id,
                query=query,
                status=success,
                latency=latency,
                length=content_length,
                error_info=error_info[:200]  # 限制错误信息长度
            )

            # 保存详细错误日志
            if not success:
                save_error(
                    service=service_name,
                    query=query,
                    req_id=req_id,
                    status=status_code,
                    content=b''.join(content_sample).decode('utf-8', 'ignore')[:500],
                    exception=error_details
                )
        except Exception as inner_e:
            print(f"⚠️ 后处理错误: {str(inner_e)[:200]}")

    return success


def save_error(service, query, req_id, status, content, exception=None):
    error_dir = os.path.join(service, "errors")
    os.makedirs(error_dir, exist_ok=True)
    log_file = os.path.join(error_dir, f"errors_{time.strftime('%Y%m%d%H')}.log")

    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n{'=' * 40}\n")
        f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Service: {service}\nRequestID: {req_id}\nQuery: {query}\n")
        if exception:
            f.write(f"Exception: {str(exception)[:200]}\n")
        else:
            safe_status = status if status is not None else "N/A"
            f.write(f"Status: {safe_status}\nContent Length: {len(content) if content else 0}\n")
        safe_content = content if isinstance(content, str) else str(content)
        f.write(f"Content Sample: {safe_content[:200]}...\n")


# ================== 服务运行模块 ==================
def run_service(service_name):
    print(f"\n▶ 启动服务: {service_name.upper()}")
    monitor = LiveMonitor(DURATION)
    recorder = DataRecorder(service_name)
    shutdown_coord = ShutdownCoordinator()
    end_time = time.time() + DURATION
    req_id = 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        pending_sem = BoundedSemaphore(CONCURRENCY * 2)
        futures = set()

        try:
            while time.time() < end_time and not shutdown_coord.should_stop():
                # 流量控制检查
                if pending_sem.acquire(blocking=False):
                    query = random.choice(queries)
                    future = executor.submit(
                        request_handler,
                        service_name,
                        req_id,
                        query,
                        monitor,
                        recorder,
                        shutdown_coord
                    )
                    req_id += 1
                    futures.add(future)
                    future.add_done_callback(lambda f: pending_sem.release())
                else:
                    time.sleep(0.01)

                # 定期清理已完成任务
                done = {f for f in futures if f.done()}
                futures -= done

                # 动态调整检查频率
                check_interval = max(0.1, len(futures) / CONCURRENCY * 0.05)
                time.sleep(check_interval)

        except KeyboardInterrupt:
            shutdown_coord.request_shutdown()

        finally:
            # ================== 四阶段关闭流程 ==================
            print("\n🔁 进入优雅关闭流程...")

            # 阶段1: 停止新请求提交
            shutdown_coord.request_shutdown()
            print(f"[关闭阶段1] 停止新请求 | 活跃连接: {shutdown_coord.active_connections()}")

            # 阶段2: 取消未开始任务
            canceled = 0
            for future in list(futures):
                if future.cancel():
                    futures.remove(future)
                    canceled += 1
            print(f"[关闭阶段2] 取消{canceled}个未开始任务 | 剩余任务: {len(futures)}")

            # 阶段3: 等待进行中任务（分级超时）
            shutdown_start = time.time()
            timeout_levels = [
                (5, "耐心等待"),
                (10, "温和催促"),
                (15, "最终警告"),
                (MAX_REQUEST_TIMEOUT, "强制终止")
            ]

            for timeout, stage_name in timeout_levels:
                print(f"[关闭阶段3-{stage_name}] 最多等待{timeout}秒...")
                start_wait = time.time()
                while futures and (time.time() - start_wait < timeout):
                    done, _ = concurrent.futures.wait(
                        futures,
                        timeout=1,
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for f in done:
                        futures.remove(f)
                    remaining = len(futures)
                    if remaining:
                        print(f"剩余任务: {remaining} | 最长等待: {timeout - (time.time() - start_wait):.1f}s")
                    else:
                        break
                if not futures:
                    break

            # 阶段4: 强制终止顽固任务
            if futures:
                print(f"[关闭阶段4] 强制终止{len(futures)}个顽固任务")
                for f in futures:
                    f.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                futures.clear()

    # 最终处理
    monitor.stop()
    avg_success, success_rate = monitor.get_final_stats()
    recorder.close(avg_success, success_rate)

    # 连接资源最终检查
    remaining_conns = shutdown_coord.active_connections()
    if remaining_conns > 0:
        print(f"⚠️ 警告: 仍有{remaining_conns}个网络连接未正常关闭")

    print(f"\n✅ 服务 {service_name.upper()} 关闭完成 | 结果保存至: {service_name}/")


# ================== 主程序入口 ==================
if __name__ == "__main__":
    print("""
    ╔══════════════════════════════╗
    ║      分布式压力测试系统      ║
    ╚══════════════════════════════╝
    """)
    print(f"• 运行时长: {DURATION}秒")
    print(f"• 并发线程: {CONCURRENCY}")
    print(f"• 内容阈值: {MIN_CONTENT_LENGTH / 1024}KB")
    print(f"• 监控间隔: {STATS_INTERVAL}秒")
    print(f"• 测试服务: {', '.join(SERVICES_TO_RUN)}\n")

    start_time = time.time()
    for service in SERVICES_TO_RUN:
        run_service(service)
    total_time = time.time() - start_time

    print(f"\n✅ 所有测试任务完成！实际运行时间: {total_time:.2f}秒 (配置: {DURATION}秒)")
    print(f"最终报告已保存到服务目录的 requests.csv 文件中")