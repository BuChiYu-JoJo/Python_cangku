#!/usr/bin/env python3
"""
ipinfo_load_test.py

并发带节奏请求到 http://ipinfo.io/json，并把每次请求的结果（URL、最终 URL、ip、country、耗时、响应大小（KB）、状态码、错误信息）写入 CSV。
支持全局配置：TOTAL_REQUESTS, CONCURRENCY, RATE_PER_SEC
分批写入 CSV（BATCH_SIZE）

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
from datetime import datetime
from aiohttp import TCPConnector
from typing import Optional

# -----------------------
# 全局配置（直接修改这些变量即可）
# -----------------------
TOTAL_REQUESTS = 100        # 总请求次数
CONCURRENCY = 50           # 并发数（最大同时进行的请求数）
RATE_PER_SEC = 10           # 节奏：每秒发起的请求次数（requests per second）
BATCH_SIZE = 1000             # 写 CSV 的批大小（缓存到这个数量后写一次）
CSV_FILE = "results.csv"    # 输出 CSV 文件名
TARGET_URL = "http://ipinfo.io/json"   # 目标 URL（固定）
REQUEST_TIMEOUT = 20        # 单次请求超时（秒）
PROXY: Optional[str] = "http://td-customer-MzsNH4f:EtXApbeko8bDT@rmmsg2sa.eu.thordata.net:9999" # 代理（示例: "http://user:pass@host:port"），若不需代理设为 None
# 例如使用你的示例代理（不建议在公共环境泄露凭据）：
# PROXY = "http://td-customer-t91FZzE-continent-as:yh9Rp5DRniu@rmmsg2sa.as.thordata.net:9999"
# -----------------------

# 最大长度的错误信息（截断）
ERROR_TRUNCATE_LEN = 300

# CSV 字段名（注意 ip 与 country 放在响应时间列之前）
CSV_FIELDS = [
    "timestamp",
    "request_index",
    "requested_url",
    "final_url",
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

async def fetch_once(session: aiohttp.ClientSession, idx: int, url: str, proxy: Optional[str]):
    """
    发起一次请求，返回字典结果（与 CSV_FIELDS 对应）
    - 从返回的 JSON 中尝试提取 'ip' 和 'country'
    - 响应内容大小以 KB 为单位（保留 3 位小数）
    """
    start = time.monotonic()
    timestamp = datetime.utcnow().isoformat() + "Z"
    requested_url = url
    final_url = ""
    status_code = ""
    response_time_s = None
    content_size_kb = 0.0
    error_message = ""
    ip_val = ""
    country_val = ""

    try:
        # 使用 session.get 并读取原始 bytes，以便计算大小
        async with session.get(url, proxy=proxy, timeout=REQUEST_TIMEOUT) as resp:
            data = await resp.read()
            elapsed = time.monotonic() - start
            response_time_s = round(elapsed, 6)
            content_size_kb = round(len(data) / 1024.0, 3)  # KB，保留 3 位小数
            status_code = resp.status
            final_url = str(resp.url)

            # 尝试解析 JSON 并提取 ip 和 country
            try:
                # 尝试用 resp.json()（更可靠），但我们已读取 bytes，故先 decode
                # 优先使用 resp.content_type 判断
                text = data.decode('utf-8', errors='ignore')
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    ip_val = parsed.get("ip", "") or ""
                    country_val = parsed.get("country", "") or ""
            except Exception:
                # 忽略解析错误，保留 ip_val 和 country_val 为空
                pass

    except Exception as e:
        elapsed = time.monotonic() - start
        response_time_s = round(elapsed, 6)
        content_size_kb = 0.0
        status_code = ""
        final_url = url
        error_message = truncate_error(e)
        # ip 和 country 保持空

    return {
        "timestamp": timestamp,
        "request_index": idx,
        "requested_url": requested_url,
        "final_url": final_url,
        "ip": ip_val,
        "country": country_val,
        "status_code": status_code,
        "response_time_s": response_time_s,
        "content_size_kb": content_size_kb,
        "error_message": error_message,
    }

async def worker_task(idx: int, session: aiohttp.ClientSession, proxy: Optional[str], sem: asyncio.Semaphore, results_q: asyncio.Queue):
    """包装 fetch_once，确保释放 semaphore 并把结果放到结果队列"""
    try:
        res = await fetch_once(session, idx, TARGET_URL, proxy)
        await results_q.put(res)
    finally:
        sem.release()

async def csv_writer(results_q: asyncio.Queue, total_expected: int, batch_size: int, csv_path: str):
    """从队列读取结果并分批写入 CSV，直到写满 total_expected 条记录"""
    written = 0
    buffer = []

    # 先写入表头（覆盖原文件）
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()

    # 以追加方式写入后续数据
    while written < total_expected:
        row = await results_q.get()
        buffer.append(row)
        written += 1

        if len(buffer) >= batch_size:
            # flush
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

    print("[writer] 写入完成，CSV 文件已生成：", csv_path)

async def schedule_requests(total: int, concurrency: int, rate_per_sec: float, proxy: Optional[str]):
    """
    调度请求：
    - 通过 await asyncio.sleep(1/rate_per_sec) 控制发起速度（节奏）
    - 使用 semaphore 控制并发数
    - 将每次请求结果放入 results_q，由 csv_writer 批量写入文件
    """
    if rate_per_sec <= 0:
        raise ValueError("rate_per_sec 必须 > 0")

    results_q: asyncio.Queue = asyncio.Queue()
    sem = asyncio.Semaphore(concurrency)

    # aiohttp session 配置
    connector = TCPConnector(limit=0, ssl=False)  # limit=0 表示无限制的连接池（并发由 semaphore 控制）
    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # 启动 csv writer
        writer_task = asyncio.create_task(csv_writer(results_q, total, BATCH_SIZE, CSV_FILE))

        tasks = []
        start_time = time.monotonic()
        print(f"[scheduler] 开始调度 {total} 个请求，rate={rate_per_sec}/s, concurrency={concurrency}")

        for i in range(1, total + 1):
            # 节奏控制：每次循环等待 1 / rate_per_sec 秒，保证每秒发起约 rate_per_sec 个请求
            await asyncio.sleep(1.0 / rate_per_sec)

            # 获取 semaphore（若当前并发已满，会在这里等待）
            await sem.acquire()

            # 启动 worker（不等待其完成），它会在完成时释放 semaphore 并放入结果
            t = asyncio.create_task(worker_task(i, session, proxy, sem, results_q))
            tasks.append(t)

            # 为避免任务数组无限制增长，我们可以定时清理已完成任务的引用
            if len(tasks) > 1000:
                tasks = [tt for tt in tasks if not tt.done()]

        # 等待所有发起的 worker 完成
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # 等待 writer 任务完成（它会在收到所有 expected 条数后退出）
        await writer_task

        elapsed = time.monotonic() - start_time
        print(f"[scheduler] 全部请求完成，用时 {elapsed:.2f} 秒")

def main():
    proxy = PROXY

    print("配置：")
    print(f"  TOTAL_REQUESTS={TOTAL_REQUESTS}")
    print(f"  CONCURRENCY={CONCURRENCY}")
    print(f"  RATE_PER_SEC={RATE_PER_SEC}")
    print(f"  BATCH_SIZE={BATCH_SIZE}")
    print(f"  CSV_FILE={CSV_FILE}")
    print(f"  TARGET_URL={TARGET_URL}")
    print(f"  PROXY={'(使用代理)' if proxy else '(不使用代理)'}")

    # 运行调度器
    asyncio.run(schedule_requests(TOTAL_REQUESTS, CONCURRENCY, RATE_PER_SEC, proxy))

if __name__ == "__main__":
    main()