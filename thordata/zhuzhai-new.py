#!/usr/bin/env python3
"""
ipinfo_load_test.py

并发带节奏请求到 http://ipinfo.io/json，并把每次请求的结果（URL、代理、ip、country、耗时、响应大小（KB）、状态码、错误信息）写入 CSV。
支持全局配置：TOTAL_REQUESTS, CONCURRENCY, RATE_PER_SEC
分批写入 CSV（BATCH_SIZE）

改动：
- 去掉 final_url 列（不再统计和记录）
- 新增 proxy_url 列：把请求时实际使用的代理字符串（即 PROXY）写入 CSV；未使用代理则为空
- 额外生成汇总 CSV：仅统计成功请求（2xx 且无 error）的平均延迟、P50、P75、P90
- RATE_PER_SEC=0 时不限制节奏（不 sleep），仅由 CONCURRENCY 限制并发
- 对超时请求：error_message 中标注 "timeout"（例如 "timeout: ..."）

依赖:
    pip install aiohttp

用法:
    python ipinfo_load_test.py
"""

import asyncio
import aiohttp
import csv
import time
import json
import math
from datetime import datetime
from aiohttp import TCPConnector
from typing import Optional, Dict, Any, List, Union

# -----------------------
# 全局配置（直接修改这些变量即可）
# -----------------------
TOTAL_REQUESTS = 1000        # 总请求次数
CONCURRENCY = 500            # 并发数（最大同时进行的请求数）
RATE_PER_SEC = 0           # 节奏：每秒发起的请求次数；设为 0 表示不限制节奏
BATCH_SIZE = 1000             # 写 CSV 的批大小（缓存到这个数量后写一次）
CSV_FILE = "results.csv"    # 明细输出 CSV 文件名
SUMMARY_CSV_FILE = "summary.csv"  # 汇总输出 CSV 文件名

TARGET_URL = "http://ipinfo.io/json"   # 目标 URL（固定）
REQUEST_TIMEOUT = 15        # 单次请求超时（秒）

PROXY: Optional[str] = "http://td-customer-MzsNH4f:EtXApbeko8bDT@rmmsg2sa.eu.thordata.net:9999"
# 若不需代理设为 None
# -----------------------

# 最大长度的错误信息（截断）
ERROR_TRUNCATE_LEN = 300

# CSV 字段名（去掉 final_url；新增 proxy_url）
CSV_FIELDS = [
    "timestamp",
    "request_index",
    "requested_url",
    "proxy_url",
    "ip",
    "country",
    "status_code",
    "response_time_s",
    "content_size_kb",
    "error_message",
]


def truncate_error(err: Exception) -> str:
    """格式化并截断错误信息，保留主要内容一行内的前若干字符。"""
    if err is None:
        return ""
    s = str(err)
    s = s.splitlines()[0] if s else ""
    if len(s) > ERROR_TRUNCATE_LEN:
        s = s[:ERROR_TRUNCATE_LEN] + "..."
    return s


def is_success(status_code: Union[int, str, None], error_message: str) -> bool:
    """成功：2xx 且无 error_message。"""
    if error_message:
        return False
    try:
        code = int(status_code)
    except Exception:
        return False
    return 200 <= code <= 299


def percentile(sorted_values: List[float], p: float) -> float:
    """
    百分位（线性插值），p in [0, 100]
    sorted_values 必须已排序
    """
    if not sorted_values:
        return float("nan")
    if p <= 0:
        return sorted_values[0]
    if p >= 100:
        return sorted_values[-1]

    n = len(sorted_values)
    pos = (n - 1) * (p / 100.0)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_values[lo]
    w = pos - lo
    return sorted_values[lo] * (1.0 - w) + sorted_values[hi] * w


async def fetch_once(session: aiohttp.ClientSession, idx: int, url: str, proxy: Optional[str]) -> Dict[str, Any]:
    """
    发起一次请求，返回字典结果（与 CSV_FIELDS 对应）
    - 从返回的 JSON 中尝试提取 'ip' 和 'country'
    - 响应内容大小以 KB 为单位（保留 3 位小数）
    - proxy_url：记录本次请求“实际使用”的代理字符串（即传入的 proxy），未使用则为空
    - 超时：error_message 标注 timeout
    """
    start = time.monotonic()
    timestamp = datetime.utcnow().isoformat() + "Z"
    requested_url = url

    # 记录“实际使用”的代理（就是你传入 session.get 的 proxy 参数）
    proxy_url = proxy or ""

    status_code: Union[int, str] = ""
    response_time_s: float = 0.0
    content_size_kb = 0.0
    error_message = ""
    ip_val = ""
    country_val = ""

    try:
        async with session.get(url, proxy=proxy, timeout=REQUEST_TIMEOUT) as resp:
            data = await resp.read()
            elapsed = time.monotonic() - start
            response_time_s = round(elapsed, 6)
            content_size_kb = round(len(data) / 1024.0, 3)
            status_code = resp.status

            # 尝试解析 JSON 并提取 ip 和 country
            try:
                text = data.decode("utf-8", errors="ignore")
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    ip_val = parsed.get("ip", "") or ""
                    country_val = parsed.get("country", "") or ""
            except Exception:
                pass

    except asyncio.TimeoutError as e:
        elapsed = time.monotonic() - start
        response_time_s = round(elapsed, 6)
        content_size_kb = 0.0
        status_code = ""
        msg = truncate_error(e) or "request timeout"
        error_message = f"timeout: {msg}"

    except aiohttp.ClientTimeout as e:
        elapsed = time.monotonic() - start
        response_time_s = round(elapsed, 6)
        content_size_kb = 0.0
        status_code = ""
        msg = truncate_error(e) or "request timeout"
        error_message = f"timeout: {msg}"

    except Exception as e:
        elapsed = time.monotonic() - start
        response_time_s = round(elapsed, 6)
        content_size_kb = 0.0
        status_code = ""
        error_message = truncate_error(e)

    return {
        "timestamp": timestamp,
        "request_index": idx,
        "requested_url": requested_url,
        "proxy_url": proxy_url,
        "ip": ip_val,
        "country": country_val,
        "status_code": status_code,
        "response_time_s": response_time_s,
        "content_size_kb": content_size_kb,
        "error_message": error_message,
    }


async def worker_task(
    idx: int,
    session: aiohttp.ClientSession,
    proxy: Optional[str],
    sem: asyncio.Semaphore,
    results_q: asyncio.Queue,
):
    """包装 fetch_once，确保释放 semaphore 并把结果放到结果队列"""
    try:
        res = await fetch_once(session, idx, TARGET_URL, proxy)
        await results_q.put(res)
    finally:
        sem.release()


async def csv_writer(
    results_q: asyncio.Queue,
    total_expected: int,
    batch_size: int,
    csv_path: str,
    summary_csv_path: str,
):
    """
    从队列读取结果并分批写入明细 CSV，直到写满 total_expected 条记录。
    同时收集成功请求的 response_time_s，最后输出汇总 CSV。
    """
    written = 0
    buffer: List[Dict[str, Any]] = []

    success_latencies: List[float] = []
    success_count = 0

    # 先写入表头（覆盖原文件）
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()

    # 以追加方式写入后续数据
    while written < total_expected:
        row = await results_q.get()
        buffer.append(row)
        written += 1

        if is_success(row.get("status_code"), row.get("error_message", "")):
            success_count += 1
            try:
                success_latencies.append(float(row.get("response_time_s", 0.0)))
            except Exception:
                pass

        if len(buffer) >= batch_size:
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                for r in buffer:
                    writer.writerow(r)
            print(f"[writer] 已写入 {written}/{total_expected} 条（批量写入 {len(buffer)} 条）")
            buffer.clear()

    # 写剩余未满批次的数据
    if buffer:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            for r in buffer:
                writer.writerow(r)
        print(f"[writer] 最终写入剩余 {len(buffer)} 条，累计 {written}/{total_expected} 条")

    print("[writer] 明细写入完成，CSV 文件已生成：", csv_path)

    # 汇总：仅统计成功请求
    success_latencies.sort()
    avg_latency = (sum(success_latencies) / len(success_latencies)) if success_latencies else float("nan")
    p50 = percentile(success_latencies, 50)
    p75 = percentile(success_latencies, 75)
    p90 = percentile(success_latencies, 90)

    summary_fields = [
        "timestamp_utc",
        "target_url",
        "total_requests",
        "success_requests",
        "success_rate",
        "avg_latency_s",
        "p50_latency_s",
        "p75_latency_s",
        "p90_latency_s",
    ]

    summary_row = {
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "target_url": TARGET_URL,
        "total_requests": total_expected,
        "success_requests": success_count,
        "success_rate": round(success_count / total_expected, 6) if total_expected else 0.0,
        "avg_latency_s": round(avg_latency, 6) if not math.isnan(avg_latency) else "",
        "p50_latency_s": round(p50, 6) if not math.isnan(p50) else "",
        "p75_latency_s": round(p75, 6) if not math.isnan(p75) else "",
        "p90_latency_s": round(p90, 6) if not math.isnan(p90) else "",
    }

    with open(summary_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerow(summary_row)

    print("[writer] 汇总写入完成，Summary CSV 文件已生成：", summary_csv_path)


async def schedule_requests(total: int, concurrency: int, rate_per_sec: float, proxy: Optional[str]):
    """
    调度请求：
    - rate_per_sec > 0：sleep(1/rate_per_sec) 控制发起速度（节奏）
    - rate_per_sec <= 0：不限制节奏（不 sleep），仅由 semaphore 控制并发
    - 将每次请求结果放入 results_q，由 csv_writer 批量写入文件并输出汇总
    """
    results_q: asyncio.Queue = asyncio.Queue()
    sem = asyncio.Semaphore(concurrency)

    connector = TCPConnector(limit=0, ssl=False)  # 并发由 semaphore 控制
    timeout = aiohttp.ClientTimeout(total=None)

    pacing_enabled = rate_per_sec and rate_per_sec > 0
    interval = (1.0 / rate_per_sec) if pacing_enabled else 0.0

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        writer_task = asyncio.create_task(
            csv_writer(results_q, total, BATCH_SIZE, CSV_FILE, SUMMARY_CSV_FILE)
        )

        tasks: List[asyncio.Task] = []
        start_time = time.monotonic()
        print(
            f"[scheduler] 开始调度 {total} 个请求，"
            f"rate={'不限速' if not pacing_enabled else str(rate_per_sec) + '/s'}, "
            f"concurrency={concurrency}"
        )

        for i in range(1, total + 1):
            if pacing_enabled:
                await asyncio.sleep(interval)

            await sem.acquire()
            t = asyncio.create_task(worker_task(i, session, proxy, sem, results_q))
            tasks.append(t)

            if len(tasks) > 1000:
                tasks = [tt for tt in tasks if not tt.done()]

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        await writer_task

        elapsed = time.monotonic() - start_time
        print(f"[scheduler] 全部请求完成，用时 {elapsed:.2f} 秒")


def main():
    proxy = PROXY

    print("配置：")
    print(f"  TOTAL_REQUESTS={TOTAL_REQUESTS}")
    print(f"  CONCURRENCY={CONCURRENCY}")
    print(f"  RATE_PER_SEC={RATE_PER_SEC} ({'不限速' if RATE_PER_SEC == 0 else '限速'})")
    print(f"  BATCH_SIZE={BATCH_SIZE}")
    print(f"  CSV_FILE={CSV_FILE}")
    print(f"  SUMMARY_CSV_FILE={SUMMARY_CSV_FILE}")
    print(f"  TARGET_URL={TARGET_URL}")
    print(f"  PROXY={'(使用代理)' if proxy else '(不使用代理)'}")

    asyncio.run(schedule_requests(TOTAL_REQUESTS, CONCURRENCY, RATE_PER_SEC, proxy))


if __name__ == "__main__":
    main()
