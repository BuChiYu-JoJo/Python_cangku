# -*- coding: utf-8 -*-
"""
测试脚本：基于 URL 模式（参考 rep_01_06_profiles_url.py 的传参与调用）
目标入口：
    from rep_01_06_profiles_url import youtube_multiple_results_profiles_url

调用签名（返回 7 元组）：
    async def youtube_multiple_results_profiles_url(user_inputs, cos_key, error, retry_times=5)

参数结构（URL 模式，单子任务版）：
    user_inputs = {
        "spider_id": str,
        "spider_errors": "true"/"false",
        "spider_parameters": [
            {"url": "https://www.youtube.com/@xxxx", "proxy_region": "us"}
        ]
    }


"""

import os
import csv
import time
import json
import asyncio
import importlib.util
from datetime import datetime
from typing import List, Dict, Any

# ============== 基本配置 ==============
# 被测脚本路径（URL 模式）
DEFAULT_TARGET = "./rep_01_06_profiles_url.py"

# 总调用次数（>len(TASKS) 会循环使用）
TOTAL_CALLS = 3

# 每次调用之间的间隔（秒）
DELAY_BETWEEN_CALLS = 1

# 重试次数（与被测函数入参一致）
RETRY_TIMES = 0

# 输出目录（CSV 会写在此目录下，文件名带时间戳）
OUTPUT_DIR = "./yt_profiles_url_results"

# 任务列表：每个元素就是“一次调用”的完整参数（仅 1 个 url -> 1 个子任务）
# URL 选取公开且稳定的频道主页（@handle 或 /channel/ID），避免跳转页。
TASKS: List[Dict[str, Any]] = [
    # 北美 / US
    {
        "desc": "US | Google 官方频道",
        "spider_id": "yt_us_google",
        "spider_errors": "false",
        "proxy_region": "us",
        "url": "https://www.youtube.com/@Google",
    },
    {
        "desc": "US | TED 演讲",
        "spider_id": "yt_us_ted",
        "spider_errors": "false",
        "proxy_region": "us",
        "url": "https://www.youtube.com/@TED",
    },

    # 欧洲 / EU
    {
        "desc": "EU | BBC News",
        "spider_id": "yt_eu_bbcnews",
        "spider_errors": "false",
        "proxy_region": "eu",
        "url": "https://www.youtube.com/@BBCNews",
    },
    {
        "desc": "EU | UEFA 欧足联",
        "spider_id": "yt_eu_uefa",
        "spider_errors": "false",
        "proxy_region": "eu",
        "url": "https://www.youtube.com/@UEFA",
    },

    # 亚洲 / AS
    {
        "desc": "AS | Sony（日本）",
        "spider_id": "yt_as_sonyjp",
        "spider_errors": "false",
        "proxy_region": "as",
        "url": "https://www.youtube.com/@Sony",
    },
    {
        "desc": "AS | Bloomberg（亚洲频道）",
        "spider_id": "yt_as_bloomberg",
        "spider_errors": "false",
        "proxy_region": "as",
        "url": "https://www.youtube.com/@BloombergTVAsia",
    },

    # 北美 / NA
    {
        "desc": "NA | NBA 官方",
        "spider_id": "yt_na_nba",
        "spider_errors": "false",
        "proxy_region": "na",
        "url": "https://www.youtube.com/@NBA",
    },
    {
        "desc": "NA | NASA",
        "spider_id": "yt_na_nasa",
        "spider_errors": "false",
        "proxy_region": "na",
        "url": "https://www.youtube.com/@NASA",
    },

    # 大洋洲 / OC
    {
        "desc": "OC | 澳大利亚旅游局",
        "spider_id": "yt_oc_aus",
        "spider_errors": "false",
        "proxy_region": "oc",
        "url": "https://www.youtube.com/@australia",
    },
    {
        "desc": "OC | 新西兰旅游",
        "spider_id": "yt_oc_nz",
        "spider_errors": "false",
        "proxy_region": "oc",
        "url": "https://www.youtube.com/@purenewzealand",
    },

    # 其他稳定大频道，增强多样性
    {
        "desc": "US | Microsoft",
        "spider_id": "yt_us_msft",
        "spider_errors": "false",
        "proxy_region": "us",
        "url": "https://www.youtube.com/@Microsoft",
    },
    {
        "desc": "EU | Mercedes-Benz",
        "spider_id": "yt_eu_mercedes",
        "spider_errors": "false",
        "proxy_region": "eu",
        "url": "https://www.youtube.com/@MercedesBenz",
    },
    {
        "desc": "AS | Samsung",
        "spider_id": "yt_as_samsung",
        "spider_errors": "false",
        "proxy_region": "as",
        "url": "https://www.youtube.com/@Samsung",
    },
    {
        "desc": "NA | Apple",
        "spider_id": "yt_na_apple",
        "spider_errors": "false",
        "proxy_region": "na",
        "url": "https://www.youtube.com/@Apple",
    },
    {
        "desc": "EU | UEFA Champions League",
        "spider_id": "yt_eu_ucl",
        "spider_errors": "false",
        "proxy_region": "eu",
        "url": "https://www.youtube.com/@UEFAChampionsLeague",
    },
    {
        "desc": "AS | 日本旅游局",
        "spider_id": "yt_as_jnto",
        "spider_errors": "false",
        "proxy_region": "as",
        "url": "https://www.youtube.com/@visitjapan",
    },
]

# ============== 工具函数 ==============
def load_target_module(path: str):
    """动态加载被测脚本 rep_01_06_profiles_url.py"""
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"找不到被测脚本：{abs_path}")
    spec = importlib.util.spec_from_file_location("target_module", abs_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def make_user_inputs_single(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    构造与 rep_01_06_profiles_url.py 适配的 user_inputs（单子任务：仅 1 个 url）
    参考其 handle_parameter：每个 user_parm 需要包含 url；可带 proxy_region；顶层需要 spider_id / spider_errors。
    """
    url = (task.get("url") or "").strip()
    if not url:
        raise ValueError("未提供有效的 url")

    return {
        "spider_id": task["spider_id"],
        "spider_errors": task.get("spider_errors", "false"),
        "spider_parameters": [
            {
                "url": url,
                "proxy_region": task.get("proxy_region", "us"),
            }
        ],
    }


def make_cos_key() -> str:
    now = datetime.now()
    return f"scrapers/thordata/{now:%Y/%m/%d}/task_{now:%Y%m%d%H%M%S}_{os.getpid()}"


def ensure_csv() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, f"yt_profiles_url_{datetime.now():%Y%m%d_%H%M%S}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "ts",
            "desc",
            "spider_id",
            "url",
            "proxy_region",
            "spider_errors",
            "total_number",
            "len_result",
            "error_number",
            "error_rate",
            "size_in_bytes",
            "total_time",
            "success",
            "error_msg",   # 新增列
        ])
    return csv_path


async def call_once(mod, task: Dict[str, Any]):
    user_inputs = make_user_inputs_single(task)
    cos_key = make_cos_key()
    error_set = set()

    print("\n[CALL] 配置说明:", task["desc"])
    print("[CALL] user_inputs:", json.dumps(user_inputs, ensure_ascii=False))

    total_number, size_in_bytes, total_time, error_rate, error_number, \
        len_result, error = await mod.youtube_multiple_results_profiles_url(
            user_inputs, cos_key, error_set, retry_times=RETRY_TIMES
        )

    success = 1 if (int(error_number) == 0 and int(len_result) > 0) else 0

    return [
        datetime.now().isoformat(timespec="seconds"),
        task["desc"],
        task["spider_id"],
        task["url"],
        task.get("proxy_region", "us"),
        str(task.get("spider_errors", "false")),
        int(total_number),
        int(len_result),
        int(error_number),
        float(error_rate),
        int(size_in_bytes),
        round(float(total_time), 3),
        success,
        str(error),   # 记录错误信息
    ]


async def main_async():
    mod = load_target_module(DEFAULT_TARGET)
    csv_path = ensure_csv()
    rows = []

    for i in range(TOTAL_CALLS):
        task = TASKS[i % len(TASKS)]
        print(f"\n========== Run {i+1}/{TOTAL_CALLS} ==========")
        row = await call_once(mod, task)
        rows.append(row)
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)
        if i < TOTAL_CALLS - 1 and DELAY_BETWEEN_CALLS > 0:
            time.sleep(DELAY_BETWEEN_CALLS)

    # 汇总
    succ = sum(r[-1] for r in rows)
    avg_time = round(sum(r[11] for r in rows) / max(len(rows), 1), 3)  # total_time
    total_bytes = sum(r[10] for r in rows)  # size_in_bytes

    print("\n========== 汇总 ==========")
    print(f"  调用次数（=子任务数）: {TOTAL_CALLS}")
    print(f"  成功次数             : {succ}")
    print(f"  成功率               : {round(succ / TOTAL_CALLS, 2) if TOTAL_CALLS else 0.0}")
    print(f"  平均总耗时(s)        : {avg_time}")
    print(f"  总字节数             : {total_bytes}")
    print(f"  CSV 路径             : {os.path.abspath(csv_path)}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
