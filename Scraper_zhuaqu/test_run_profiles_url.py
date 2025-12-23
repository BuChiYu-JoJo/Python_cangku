# -*- coding: utf-8 -*-
"""
测试脚本（方案一·合并版，统一 desc/spider_id + 更完整错误记录）
- 单个 CSV 文件：前面是每次调用明细，最后一行是汇总
- 失败时 error_msg 兜底写入（HTTP 状态码/错误码/异常）
- desc 和 spider_id 不再带地区，仅使用频道名
- 可选直连：FORCE_DIRECT=True 时禁用代理（更稳定）
"""

import os
import csv
import time
import json
import asyncio
import importlib.util
from datetime import datetime
from typing import List, Dict, Any, Optional

# ============== 基本配置 ==============
DEFAULT_TARGET = "./rep_01_06_profiles_url.py"
TOTAL_CALLS = 100
DELAY_BETWEEN_CALLS = 0.5
RETRY_TIMES = 3
OUTPUT_DIR = "./yt_profiles_direct_results"

# 如需稳定直连（不走代理池），把这个开关设为 True
FORCE_DIRECT = False  # True => 禁用代理；False => 按模块默认（通常是 US 代理）

# ============== 任务列表（统一 desc / spider_id） ==============
TASKS: List[Dict[str, Any]] = [
    {"desc": "Google",         "spider_id": "google",     "url": "https://www.youtube.com/@Google"},
    {"desc": "TED",            "spider_id": "ted",        "url": "https://www.youtube.com/@TED"},
    {"desc": "BBC News",       "spider_id": "bbcnews",    "url": "https://www.youtube.com/@BBCNews"},
    {"desc": "NBA",            "spider_id": "nba",        "url": "https://www.youtube.com/@NBA"},
    {"desc": "NASA",           "spider_id": "nasa",       "url": "https://www.youtube.com/@NASA"},
    {"desc": "Mercedes-Benz",  "spider_id": "mercedes",   "url": "https://www.youtube.com/@MercedesBenz"},
]

# ============== 工具函数 ==============
def load_target_module(path: str):
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"找不到被测脚本：{abs_path}")
    spec = importlib.util.spec_from_file_location("target_module", abs_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod

def make_spider_inputs(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    构造 spider_inputs（与 rep_01_06_profiles_url.py 的 main 一致的结构）
    统一使用 'us'，外层不再带地区概念
    """
    url = (task.get("url") or "").strip()
    if not url:
        raise ValueError("未提供有效 url")

    return {
        "user_input": {"url": url, "proxy_region": "us"},
        "spider_input": {
            "spider_errors": False,          # 用布尔，避免字符串真假歧义
            "proxy_region": "us",            # 统一 us
            "url": url,
            "user_input_id": task["spider_id"],
            "spider_item": {},
            "spider_url": url,
        },
        "discovery_input": {},
    }

def ensure_csv() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(OUTPUT_DIR, f"yt_profiles_direct_{ts}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "ts", "desc", "spider_id", "url",
            "resp_status", "success", "elapsed_sec", "resp_bytes", "error_msg"
        ])
    return csv_path

def extract_error_msg(result: Dict[str, Any]) -> str:
    """
    失败场景下尽量给出明确的错误信息：
    - 优先取 error / message / err_msg
    - 再取 error_code
    - 最后兜底用 HTTP {resp_status}
    """
    if not isinstance(result, dict):
        return "Unknown error: invalid result type"

    for k in ("error", "message", "err_msg"):
        v = result.get(k)
        if v:
            return str(v)

    if result.get("error_code") is not None:
        return f"error_code={result.get('error_code')}"

    if result.get("resp_status"):
        return f"HTTP {result.get('resp_status')}"

    return ""

async def call_once(mod, task: Dict[str, Any]):
    spider_inputs = make_spider_inputs(task)

    print("\n[CALL] 配置说明:", task["desc"])
    t0 = time.time()
    try:
        result = await mod.get_youtube_page_content(
            spider_inputs, cookies=None, impersonate=True, retry_times=RETRY_TIMES
        )
    except Exception as e:
        elapsed = time.time() - t0
        err = f"EXC {type(e).__name__}: {e}"
        return [
            datetime.now().isoformat(timespec="seconds"),
            task["desc"],
            task["spider_id"],
            task["url"],
            "",  # resp_status
            0,   # success
            round(elapsed, 3),
            0,   # resp_bytes
            err,
        ], 0, elapsed, 0

    elapsed = time.time() - t0
    success = 1 if result.get("success", False) else 0
    resp_status = result.get("resp_status", "")

    error_msg = ""
    if not success:
        error_msg = extract_error_msg(result)

    try:
        resp_bytes = len(json.dumps(result, ensure_ascii=False).encode("utf-8"))
    except Exception:
        resp_bytes = 0

    return [
        datetime.now().isoformat(timespec="seconds"),
        task["desc"],
        task["spider_id"],
        task["url"],
        resp_status if resp_status else "",
        success,
        round(elapsed, 3),
        resp_bytes,
        error_msg,
    ], success, elapsed, resp_bytes

async def main_async():
    mod = load_target_module(DEFAULT_TARGET)

    # 如需稳定直连，打开 FORCE_DIRECT 将禁用代理（推荐先试直连是否稳定）
    if FORCE_DIRECT and hasattr(mod, "get_dongtai_proxy"):
        mod.get_dongtai_proxy = lambda *a, **kw: None
        print("[patch] 已禁用代理（直连）")

    csv_path = ensure_csv()
    rows = []
    succ_cnt, total_bytes, total_time = 0, 0, 0.0
    start_run = time.time()

    for i in range(TOTAL_CALLS):
        task = TASKS[i % len(TASKS)]
        row, s, elapsed, b = await call_once(mod, task)
        rows.append(row)
        succ_cnt += s
        total_bytes += b
        total_time += elapsed
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)
        if i < TOTAL_CALLS - 1 and DELAY_BETWEEN_CALLS > 0:
            time.sleep(DELAY_BETWEEN_CALLS)

    total_calls = len(rows)
    err_cnt = total_calls - succ_cnt
    error_rate = round(err_cnt / total_calls, 4) if total_calls else 0.0
    avg_time = round(total_time / total_calls, 3) if total_calls else 0.0
    avg_bytes = int(total_bytes / total_calls) if total_calls else 0

    # 在同一 CSV 末尾写汇总
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([])
        w.writerow([
            "汇总", f"total_calls={total_calls}", f"success={succ_cnt}",
            f"errors={err_cnt}", f"error_rate={error_rate}",
            f"total_time={round(total_time,3)}", f"total_bytes={total_bytes}",
            f"avg_time={avg_time}", f"avg_bytes={avg_bytes}"
        ])

    print("\n========== 汇总 ==========")
    print(f"  调用次数 : {total_calls}")
    print(f"  成功次数 : {succ_cnt}")
    print(f"  错误次数 : {err_cnt}")
    print(f"  错误率   : {error_rate}")
    print(f"  总耗时(s): {round(total_time,3)}")
    print(f"  总字节数 : {total_bytes}")
    print(f"  CSV 路径 : {os.path.abspath(csv_path)}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
