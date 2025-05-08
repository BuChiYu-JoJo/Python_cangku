import requests
import concurrent.futures
import csv
import time
import os
import threading
import itertools
from urllib.parse import quote
from collections import deque

# 全局配置
DURATION = 10 * 60 * 60  # 运行时长（秒）
MIN_CONTENT_LENGTH = 15 * 1024  # 15KB阈值
CONCURRENCY = 50  # 并发线程数
queries = ["pizza"]  # 查询关键词
STATS_INTERVAL = 1  # 统计显示间隔（秒）

# ================== 实时监控模块 ==================
class LiveMonitor:
    def __init__(self, duration):
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.duration = duration  # 总运行时长

        # 实时统计指标
        self.total = 0
        self.success = 0
        self.fail = 0
        self.latencies = deque(maxlen=1000)  # 保留最近1000次请求的延迟
        self.success_latency_sum = 0  # 成功请求总延迟
        self.success_count = 0  # 成功请求次数

        # QPS计算相关
        self.last_total = 0
        self.last_time = self.start_time

        # 历史记录
        self.history = {
            'timestamps': [],
            'success_counts': [],
            'fail_counts': [],
            'rps_values': []
        }

        # 启动监控线程
        self._running = True
        threading.Thread(target=self._display_loop, daemon=True).start()

    def _display_loop(self):
        """实时显示统计信息"""
        while self._running:
            time.sleep(STATS_INTERVAL)
            self._print_stats()

    def _print_stats(self):
        """格式化输出统计信息"""
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.start_time
            remaining = max(0, self.duration - elapsed)
            avg_latency = sum(self.latencies)/len(self.latencies) if self.latencies else 0

            # 计算实时QPS
            time_diff = current_time - self.last_time
            current_total = self.total
            current_rps = (current_total - self.last_total) / time_diff if time_diff > 0 else 0
            self.last_total = current_total
            self.last_time = current_time

            # 计算平均成功延迟
            avg_success = self.success_latency_sum / self.success_count if self.success_count else 0

            # 构建控制台显示
            display = [
                "\n" + "="*50,
                f"实时监控 @ {time.strftime('%Y-%m-%d %H:%M:%S')}",
                "-"*50,
                f"运行时间: {self._format_duration(elapsed)} | 剩余时间: {self._format_duration(remaining)}",
                f"总请求量: {self.total} | 成功: {self.success} | 失败: {self.fail}",
                f"成功率: {self.success/self.total*100:.2f}%" if self.total else "成功率: N/A",
                f"实时QPS: {current_rps:.2f}/s | 平均延迟: {avg_latency:.2f}ms | 平均成功延迟: {avg_success:.2f}ms",
                "="*50
            ]
            os.system('cls' if os.name == 'nt' else 'clear')
            print("\n".join(display))

    def _format_duration(self, seconds):
        """格式化时间显示"""
        hours, rem = divmod(seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    def record_success(self, latency):
        with self.lock:
            self.total += 1
            self.success += 1
            self.success_latency_sum += latency
            self.success_count += 1
            self.latencies.append(latency)

    def record_failure(self, latency):
        with self.lock:
            self.total += 1
            self.fail += 1
            self.latencies.append(latency)

    def get_avg_success_latency(self):
        with self.lock:
            if self.success_count == 0:
                return 0.0
            return self.success_latency_sum / self.success_count

    def get_final_stats(self):
        with self.lock:
            if self.total == 0:
                return 0.0, 0.0
            success_rate = (self.success / self.total) * 100
            avg_success = self.success_latency_sum / self.success_count if self.success_count else 0.0
            return avg_success, success_rate

    def stop(self):
        self._running = False
        self._print_stats()

# ================== 数据记录模块 ==================
class DataRecorder:
    def __init__(self, service_name):
        self.service = service_name
        os.makedirs(service_name, exist_ok=True)

        # 初始化CSV文件
        self.csv_file = open(f'{service_name}/requests.csv', 'w', newline='', encoding='utf-8')
        self.writer = csv.writer(self.csv_file)
        self.writer.writerow([
            'Timestamp', 'RequestID', 'Query', 
            'Status', 'Latency(ms)', 'ContentLength', 'AvgSuccessLatency(ms)', '请求成功率'
        ])

    def log_request(self, req_id, query, status, latency, length):
        self.writer.writerow([
            time.strftime('%Y-%m-%d %H:%M:%S'),
            req_id,
            query,
            'Success' if status else 'Failed',
            f"{latency:.2f}",
            length,
            "",  # AvgSuccessLatency留空
            ""   # 请求成功率留空
        ])

    def close(self, avg_success_latency, success_rate):
        self.csv_file.close()
        # 追加最终统计行
        with open(f'{self.service}/requests.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                "", "", "", "", "", "",  # 前六个字段留空
                f"{avg_success_latency:.2f}",
                f"{success_rate:.2f}%"
            ])

# ================== 请求处理模块 ==================
def get_session():
    # 每个线程创建独立的Session对象
    thread_local = threading.local()
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
    return thread_local.session

def request_handler(service_name, req_id, query, monitor, recorder):
    session = get_session()
    start_time = time.time()
    try:
        url = f"http://170.106.180.18:9004/spiders?search_url={quote(f'https://www.bing.com/search?q={query}', safe=':/?=')}"
        response = session.get(url, timeout=30)
        latency = (time.time() - start_time) * 1000
        content = response.text
        response.close()  # 显式关闭响应

        if response.status_code == 200 and len(content) >= MIN_CONTENT_LENGTH:
            monitor.record_success(latency)
            recorder.log_request(req_id, query, True, latency, len(content))
            return True

        # 记录错误请求
        monitor.record_failure(latency)
        recorder.log_request(req_id, query, False, latency, len(content))
        save_error(service_name, query, req_id, response.status_code, content)
        return False

    except Exception as e:
        latency = (time.time() - start_time) * 1000
        monitor.record_failure(latency)
        recorder.log_request(req_id, query, False, latency, 0)
        save_error(service_name, query, req_id, None, "", e)
        return False

def save_error(service, query, req_id, status, content, exception=None):
    """错误保存逻辑"""
    error_dir = os.path.join(service, "errors")
    os.makedirs(error_dir, exist_ok=True)

    filename = f"{query}_{req_id}_{status if status else 'EXCEPTION'}"
    if exception:
        filename += f"_{type(exception).__name__}"
    filename += ".txt"

    with open(os.path.join(error_dir, filename), 'w', encoding='utf-8') as f:
        f.write(f"Error Report\n{'='*40}\n")
        f.write(f"Service: {service}\nRequestID: {req_id}\nQuery: {query}\n")
        if exception:
            f.write(f"Exception: {str(e)}\n")
        else:
            f.write(f"Status: {status}\nContent Length: {len(content)}\n")
        f.write("\nResponse Content:\n" + content)

# ================== 服务运行模块 ==================
def run_service(service_name):
    print(f"\n▶ 启动服务: {service_name.upper()}")

    monitor = LiveMonitor(DURATION)
    recorder = DataRecorder(service_name)
    query_cycle = itertools.cycle(queries)
    end_time = time.time() + DURATION
    req_id = 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = set()

        while time.time() < end_time:
            # 持续提交任务直到达到并发数或时间结束
            while len(futures) < CONCURRENCY and time.time() < end_time:
                query = next(query_cycle)
                future = executor.submit(
                    request_handler,
                    service_name,
                    req_id,
                    query,
                    monitor,
                    recorder
                )
                futures.add(future)
                req_id += 1

            # 处理完成的任务并立即补充新任务
            done, _ = concurrent.futures.wait(
                futures,
                timeout=0.1,
                return_when=concurrent.futures.FIRST_COMPLETED
            )

            for future in done:
                try:
                    future.result()
                except Exception as e:
                    print(f"请求处理异常: {str(e)}")
                futures.remove(future)

        # 处理剩余任务
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"请求处理异常: {str(e)}")

    # 清理资源
    monitor.stop()
    avg_success, success_rate = monitor.get_final_stats()
    recorder.close(avg_success, success_rate)
    print(f"▌ 服务 {service_name.upper()} 完成 | 结果保存至: {service_name}/")

# ================== 主程序入口 ==================
if __name__ == "__main__":
    print("""
    ╔══════════════════════════════╗
    ║      分布式压力测试系统      ║
    ╚══════════════════════════════╝
    """)
    print(f"• 运行时长: {DURATION/3600:.1f}小时")
    print(f"• 并发线程: {CONCURRENCY}")
    print(f"• 内容阈值: {MIN_CONTENT_LENGTH/1024}KB")
    print(f"• 监控间隔: {STATS_INTERVAL}秒\n")

    run_service("thordata")

    print("\n✅ 所有测试任务完成！")