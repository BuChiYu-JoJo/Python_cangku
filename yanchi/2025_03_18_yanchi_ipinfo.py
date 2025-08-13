import requests
import time
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import threading
from threading import Lock
import csv
import datetime  # 新增日期模块
import itertools

# =====================
# 全局配置区（可根据需要修改）
# =====================
URL = "https://ipinfo.io/json"

PROXY_TEMPLATE = "rmmsg2sa.{as_value}.thordata.net:9999"
AUTH_TEMPLATE = "td-customer-GH43726:GH43726"

REGIONS = ["pr", "na", "eu", "as"]  # 代理区域
CONCURRENCY = 150  # 并发线程数
CONNECT_TIMEOUT = 10  # 连接超时时间(秒)
READ_TIMEOUT = 20  # 读取超时时间(秒)
BATCH_SIZE = 2000  # CSV批量写入条数
MONITOR_INTERVAL = 30  # 监控刷新间隔(秒)

# 新增全局输出路径配置
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
OUTPUT_FOLDER = os.path.join(os.getcwd(), current_date)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)  # 创建日期文件夹

# =====================
# 全局状态对象
# =====================
write_queue = Queue()
stop_event = threading.Event()
file_lock = Lock()
monitor_data = {
    "混播": {"count": 0, "latest": []},
    "美洲": {"count": 0, "latest": []},
    "欧洲": {"count": 0, "latest": []},
    "亚洲": {"count": 0, "latest": []},
}
monitor_lock = Lock()

# =====================
# 核心功能函数
# =====================
def writer_thread():
    """CSV写入线程"""
    batch_data = {}

    while not stop_event.is_set() or not write_queue.empty():
        try:
            sheet_name, result = write_queue.get(timeout=1)

            if sheet_name not in batch_data:
                batch_data[sheet_name] = []
            batch_data[sheet_name].append(result)

            if len(batch_data[sheet_name]) >= BATCH_SIZE or (stop_event.is_set() and len(batch_data[sheet_name]) > 0):
                _write_csv_batch(sheet_name, batch_data[sheet_name])

                with monitor_lock:
                    monitor_data[sheet_name]["count"] += len(batch_data[sheet_name])
                    monitor_data[sheet_name]["latest"] = (
                        batch_data[sheet_name][-5:] +
                        monitor_data[sheet_name]["latest"][:5]
                    )

                batch_data[sheet_name] = []

        except Empty:
            continue
        except Exception as e:
            print(f"写入线程异常: {str(e)}")

    for sheet_name in list(batch_data.keys()):
        if len(batch_data[sheet_name]) > 0:
            _write_csv_batch(sheet_name, batch_data[sheet_name])
            with monitor_lock:
                if sheet_name in monitor_data:
                    monitor_data[sheet_name]["count"] += len(batch_data[sheet_name])
                    monitor_data[sheet_name]["latest"] = (
                        batch_data[sheet_name][-5:] +
                        monitor_data[sheet_name]["latest"][:5]
                    )
            del batch_data[sheet_name]

def _write_csv_batch(sheet_name, data_batch):
    """执行CSV批量写入"""
    try:
        filename = os.path.join(OUTPUT_FOLDER, f"{sheet_name}.csv")  # 路径修改
        headers = ["请求国家", "返回国家", "IP", "延迟"]

        with file_lock:
            file_exists = os.path.exists(filename)
            write_header = not file_exists or os.stat(filename).st_size == 0

            with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if write_header:
                    writer.writeheader()

                formatted = [
                    {
                        "请求国家": item["请求国家"],
#                        "大洲": item["大洲"],
                        "返回国家": item["返回国家"],
                        "IP": item["IP"],
                        "延迟": item["延迟"]
                    } for item in data_batch
                ]
                writer.writerows(formatted)
    except Exception as e:
        print(f"写入文件 {filename} 失败: {str(e)}")
        raise

def monitor_thread():
    """实时监控线程"""
    while not stop_event.is_set():
        time.sleep(MONITOR_INTERVAL)
        with monitor_lock:
            total = sum(data["count"] for data in monitor_data.values())
            if total == 0:
                continue
            print("\n" + "=" * 50)
            print(f" 监控时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 50)
            for sheet, data in monitor_data.items():
                print(f"▶ {sheet}.csv")
                print(f"   总记录数: {data['count']:>8}")
                print(f"   最新延迟样本:")
                for i, record in enumerate(data['latest'][:3], 1):
                    delay_value = record["延迟"]
                    if isinstance(delay_value, (float, int)):
                        delay_str = f"{delay_value:.2f} ms"
                    else:
                        delay_str = str(delay_value)
                    delay = delay_str.center(12)
                    country = record["请求国家"].ljust(8)
                    print(f"     {i}. {delay} | 国家: {country}")
            print("=" * 50 + "\n")

def _make_request(url, region, guojia, proxy_template, auth_template, timeout):
    """执行单个请求（动态协议代理版本）并记录详细错误日志"""
    try:
        protocol = "https" if url.startswith("https://") else "http"
        proxy_host = proxy_template.format(as_value=region)
        auth_parts = auth_template.split(":", 1)
        auth_username = auth_parts[0].format(af=guojia)
        auth_password = auth_parts[1] if len(auth_parts) > 1 else ""

        proxies = {
            protocol: f"http://{auth_username}:{auth_password}@{proxy_host}"
        }

        request_start_time = time.time()
        response = requests.get(url, proxies=proxies, timeout=timeout)
        request_end_time = time.time()
        elapsed = (request_end_time - request_start_time) * 1000  # ms

        if response.status_code == 200:
            data = response.json()
            return {
                "region": region,
                "请求国家": guojia,
                "返回国家": data.get("country", "N/A"),
                "IP": data.get("ip", "N/A"),
                "延迟": round(elapsed, 2)
            }
        else:
            error_message = f"非200状态码，返回: {response.status_code}, url: {url}, region: {region}, guojia: {guojia}, proxy: {proxy_host}"
            _log_error(error_message)
            return {
                "region": region,
                "请求国家": guojia,
                "返回国家": "N/A",
                "IP": "N/A",
                "延迟": f"HTTP_{response.status_code}"
            }

    except requests.exceptions.Timeout:
        error_message = f"请求超时，url: {url}, region: {region}, guojia: {guojia}, proxy: {proxy_host}"
        _log_error(error_message)
        return {
            "region": region,
            "请求国家": guojia,
            "返回国家": "N/A",
            "IP": "N/A",
            "延迟": "Timeout"
        }
    except Exception as e:
        error_message = f"请求异常({type(e).__name__})，url: {url}, region: {region}, guojia: {guojia}, proxy: {proxy_host}，错误详情: {str(e)}"
        _log_error(error_message)
        return {
            "region": region,
            "请求国家": guojia,
            "返回国家": "N/A",
            "IP": "N/A",
            "延迟": f"Error: {type(e).__name__}"
        }

def _log_error(message):
    """保存错误信息到日志文件"""
    log_file = os.path.join(OUTPUT_FOLDER, "error_log.txt")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}\n"
    with file_lock:  # 保证多线程写日志安全
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(full_message)

def merge_to_excel():
    """合并CSV到Excel（智能保持数值类型）"""
    print("\n开始合并CSV文件...")
    start = time.time()


    # 新增延迟列转换函数
    def convert_latency(value):
        """智能转换延迟列为数值或保留原字符串"""
        try:
            return float(value)  # 尝试转换为浮点数
        except (ValueError, TypeError):
            return str(value)    # 转换失败返回原始字符串

    sheet_mapping = {
        "pr": "混播",
        "na": "美洲",
        "eu": "欧洲",
        "as": "亚洲"
    }

    # 修改Excel输出路径
    excel_path = os.path.join(OUTPUT_FOLDER, '最终报告.xlsx')
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for region, sheet_name in sheet_mapping.items():
            csv_path = os.path.join(OUTPUT_FOLDER, f"{sheet_name}.csv")
            if os.path.exists(csv_path):
                try:
                    # 读取CSV时应用智能类型转换
                    full_df = pd.read_csv(
                        csv_path,
                        converters={
                            '延迟': convert_latency  # 关键修改：应用自定义转换
                        },
                        keep_default_na=False,
                        encoding='utf-8-sig'
                    )

                    # 写入Excel时保留混合类型
                    full_df.to_excel(
                        writer,
                        sheet_name=sheet_name,
                        index=False,
                        na_rep=""
                    )
                    print(f"成功合并: {sheet_name}.csv ({len(full_df)}条)")
                except Exception as e:
                    print(f"合并失败 {csv_path}: {str(e)}")
                    if "full_df" in locals():
                        print(f"异常数据样例:\n{full_df.head()}")
            else:
                print(f"文件不存在: {csv_path}")

    print(f"合并完成，耗时: {time.time() - start:.2f}秒")

# =====================
# 主控制函数
# =====================
def fetch_url_with_timeout():
    """主请求函数（移除国家循环，保留轮播区域，总计50000次）"""
    try:
        df = pd.read_excel("country_pd.xlsx")
        guojia_values = df['Xc'].tolist() or []
        print(f"成功读取 {len(guojia_values)} 个国家数据（当前不再使用）")
    except Exception as e:
        print(f"读取错误: {e}")
        guojia_values = []

    writer = threading.Thread(target=writer_thread)
    monitor = threading.Thread(target=monitor_thread)
    writer.start()
    monitor.start()

    total_tasks = 50000
    start_time = time.time()
    region_cycle = itertools.cycle(REGIONS)  # 区域轮播器

    try:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = []

            for _ in range(total_tasks):
                region = next(region_cycle)
                guojia = "N/A"  # 国家字段不再使用
                futures.append(
                    executor.submit(
                        _make_request,
                        URL, region, guojia,
                        PROXY_TEMPLATE, AUTH_TEMPLATE, (CONNECT_TIMEOUT, READ_TIMEOUT)
                    )
                )

            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()

                if i % 100 == 0:
                    elapsed = time.time() - start_time
                    speed = i / elapsed
                    remain = (total_tasks - i) / speed if speed > 0 else 0
                    print(
                        f"\r进度: {i}/{total_tasks} | "
                        f"速度: {speed:.1f} req/s | "
                        f"剩余: {remain / 60:.1f} min",
                        end="", flush=True
                    )

                sheet_name = {
                    "pr": "混播",
                    "na": "美洲",
                    "eu": "欧洲",
                    "as": "亚洲"
                }.get(result["region"], "混播")
                write_queue.put((sheet_name, result))

    finally:
        stop_event.set()
        writer.join()
        monitor.join()

        if not write_queue.empty():
            print(f"\n警告: 队列中残留 {write_queue.qsize()} 条数据未处理")

        merge_to_excel()

        total = sum(data["count"] for data in monitor_data.values())
        print(f"\n总处理请求: {total}")
        print(f"总耗时: {time.time() - start_time:.2f}秒")

# =====================
# 程序入口
# =====================
if __name__ == "__main__":
    fetch_url_with_timeout()