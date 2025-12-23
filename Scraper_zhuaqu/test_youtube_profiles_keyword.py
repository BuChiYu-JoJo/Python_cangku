# -*- coding: utf-8 -*-
"""
本脚本用于测试：rep_01_07_profiles_keyword.py

- 每次调用只包含 1 个子任务（仅 1 个 keyword）
- 用 TOTAL_CALLS 控制总调用次数（>len(PRESET_CONFIGS) 时会循环取用）
- 每次调用写入 CSV，并输出汇总

被测函数：
    from rep_01_07_profiles_keyword import youtube_multiple_results_profiles_keyword
签名：
    async def youtube_multiple_results_profiles_keyword(user_inputs, cos_key, error, retry_times=5)
返回：
    total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error

运行：
    python test_youtube_commend.py
"""

import os
import csv
import time
import json
import asyncio
import importlib.util
from datetime import datetime
from typing import List, Dict, Any

# ================== 基本配置（可按需修改） ==================
DEFAULT_TARGET = "./rep_01_07_profiles_keyword.py"

# 总调用次数（>len(PRESET_CONFIGS) 会循环使用）
TOTAL_CALLS = 23

# 每次调用之间的间隔（秒）
DELAY_BETWEEN_CALLS = 1

# 调用被测函数时使用的重试次数
RETRY_TIMES = 0

# 输出目录（CSV 会写在此目录下，文件名带时间戳）
OUTPUT_DIR = "./yt_profiles_results"

# 注意：每个配置只有一个 keyword -> 每次调用只有 1 个子任务
PRESET_CONFIGS: List[Dict[str, Any]] = [
    # US 技术
    {
        "desc": "US | Python 教程 | 翻2页",
        "spider_id": "yt_us_py_2",
        "spider_errors": "false",
        "domain": "https://www.youtube.com",
        "proxy_region": "us",
        "page_turning": 2,
        "keyword": "python tutorial",
    },
    {
        "desc": "US | Docker 指南 | 翻2页",
        "spider_id": "yt_us_docker_2",
        "spider_errors": "false",
        "domain": "https://www.youtube.com",
        "proxy_region": "us",
        "page_turning": 2,
        "keyword": "docker compose guide",
    },

    # EU 生活/音乐
    {
        "desc": "EU | Lofi 音乐 | 翻3页",
        "spider_id": "yt_eu_lofi_3",
        "spider_errors": "true",
        "domain": "https://www.youtube.com",
        "proxy_region": "eu",
        "page_turning": 3,
        "keyword": "lofi hip hop",
    },
#    {
#        "desc": "EU | 地中海饮食 | 翻3页",
#        "spider_id": "yt_eu_mediterranean_3",
#        "spider_errors": "true",
#        "domain": "https://www.youtube.com",
#        "proxy_region": "eu",
#        "page_turning": 3,
#        "keyword": "mediterranean diet recipes",
#    },

    # AS 科普/体育/美食
    {
        "desc": "AS | 量子计算科普 | 翻1页",
        "spider_id": "yt_as_quantum_1",
        "spider_errors": "false",
        "domain": "https://www.youtube.com",
        "proxy_region": "as",
        "page_turning": 1,
        "keyword": "quantum computing explained",
    },
    {
        "desc": "AS | 日本街头美食 | 翻1页",
        "spider_id": "yt_as_streetfood_1",
        "spider_errors": "false",
        "domain": "https://www.youtube.com",
        "proxy_region": "as",
        "page_turning": 1,
        "keyword": "japanese street food",
    },

    # 其他覆盖
    {
        "desc": "NA | AI 新闻 | 翻2页",
        "spider_id": "yt_na_ai_2",
        "spider_errors": "false",
        "domain": "https://www.youtube.com",
        "proxy_region": "na",
        "page_turning": 2,
        "keyword": "ai news",
    },
    {
        "desc": "EU | 初学者瑜伽 | 翻2页",
        "spider_id": "yt_eu_yoga_2",
        "spider_errors": "false",
        "domain": "https://www.youtube.com",
        "proxy_region": "eu",
        "page_turning": 2,
        "keyword": "yoga for beginners",
    },
]

# ================== 工具函数 ==================
def load_target_module(path: str):
    """动态加载被测脚本 rep_01_07_profiles_keyword.py"""
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到被测脚本：{path}")
    spec = importlib.util.spec_from_file_location("target_module", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def make_user_inputs(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    构造与 rep_01_07_profiles_keyword.py 适配的 user_inputs（单子任务版）:
    {
      "spider_id": str,
      "spider_errors": "true"/"false" 或 True/False,
      "spider_parameters": [
        {"keyword": "...", "domain": "...", "page_turning": 2, "proxy_region": "..."}
      ]
    }
    """
    kw = (cfg.get("keyword") or "").strip()
    if not kw:
        raise ValueError("本组配置未提供有效的 keyword")

    return {
        "spider_id": cfg["spider_id"],
        "spider_errors": cfg["spider_errors"],
        "spider_parameters": [
            {
                "keyword": kw,
                "domain": cfg["domain"],
                "page_turning": int(cfg["page_turning"]),
                "proxy_region": cfg["proxy_region"],
            }
        ],
    }


def make_cos_key() -> str:
    now = datetime.now()
    return f"scrapers/thordata/{now:%Y/%m/%d}/task_{now:%Y%m%d%H%M%S}_{os.getpid()}"


def ensure_csv() -> str:
    """创建输出目录与CSV文件，并返回CSV路径"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, f"yt_profiles_{datetime.now():%Y%m%d_%H%M%S}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "ts",
            "desc",
            "spider_id",
            "keyword",          # <- 单个 keyword
            "proxy_region",
            "page_turning",
            "spider_errors",
            "total_number",
            "len_result",
            "error_number",
            "error_rate",
            "size_in_bytes",
            "total_time",
            "success",
        ])
    return csv_path


async def call_once(mod, cfg: Dict[str, Any]):
    """
    对单次调用执行被测函数（一次仅 1 个 keyword）
    """
    user_inputs = make_user_inputs(cfg)
    cos_key = make_cos_key()
    error_set = set()

    print("\n[CALL] 配置说明:", cfg["desc"])
    print("[CALL] user_inputs:", json.dumps({
        "spider_id": user_inputs["spider_id"],
        "spider_errors": user_inputs["spider_errors"],
        "spider_parameters": user_inputs["spider_parameters"],
    }, ensure_ascii=False))

    total_number, size_in_bytes, total_time, error_rate, error_number, \
        len_result, error = await mod.youtube_multiple_results_profiles_keyword(
            user_inputs, cos_key, error_set, retry_times=RETRY_TIMES
        )

    success = 1 if (int(error_number) == 0 and int(len_result) > 0) else 0

    return [
        datetime.now().isoformat(timespec="seconds"),
        cfg["desc"],
        cfg["spider_id"],
        cfg["keyword"],                 # <- 单个 keyword
        cfg["proxy_region"],
        int(cfg["page_turning"]),
        str(cfg["spider_errors"]),
        int(total_number),
        int(len_result),
        int(error_number),
        float(error_rate),
        int(size_in_bytes),
        round(float(total_time), 3),
        success,
    ]


async def main_async():
    mod = load_target_module(DEFAULT_TARGET)
    csv_path = ensure_csv()
    rows = []

    for i in range(TOTAL_CALLS):
        cfg = PRESET_CONFIGS[i % len(PRESET_CONFIGS)]  # 轮流取配置
        print(f"\n========== Run {i+1}/{TOTAL_CALLS} ==========")
        row = await call_once(mod, cfg)
        rows.append(row)
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)
        if i < TOTAL_CALLS - 1 and DELAY_BETWEEN_CALLS > 0:
            time.sleep(DELAY_BETWEEN_CALLS)

    # 汇总
    succ = sum(r[-1] for r in rows)
    avg_time = round(sum(r[12] for r in rows) / max(len(rows), 1), 3)  # total_time
    total_bytes = sum(r[11] for r in rows)  # size_in_bytes

    print("\n========== 汇总 ==========")
    print(f"  总调用次数     : {TOTAL_CALLS}")
    print(f"  成功次数       : {succ}")
    print(f"  成功率         : {round(succ / TOTAL_CALLS, 2) if TOTAL_CALLS else 0.0}")
    print(f"  平均总耗时(s)  : {avg_time}")
    print(f"  总字节数       : {total_bytes}")
    print(f"  CSV 路径       : {os.path.abspath(csv_path)}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
