# -*- coding: utf-8 -*-
"""
Zillow 列表页筛选测试（参考 rep_02_03_filter.py）
- 正确 await get_spider_url() 生成搜索 URL
- 直接调用 get_zillow_page_content(...) 抓取列表 -> 解析详情
- 每跑完一条“筛选条件”就即时落盘到 CSV（不写汇总行）
- 输出关键指标：result_num / size_in_bytes / error_number / error_rate / elapsed_sec
"""

import os
import csv
import json
import time
import random
import asyncio
import importlib.util
from datetime import datetime
from typing import Any, Dict, List

# ============= 可配置 =============
TARGET_PATH   = "./rep_02_03_filter.py"     # 被调脚本路径（此版本基于 filter 的实现）
TOTAL_RUNS    = 3                           # ★总运行次数（每次随机挑一个筛选组合）
OUTPUT_DIR    = "./zillow_filter_results"    # 输出目录
RANDOM_SEED   = 42                           # 设 None 则每次随机不同
SPIDER_ERRORS = False                        # 是否标记 err_stats（True/False）
USER_INPUT_ID = "zillow_filter"              # 方便追踪的 user_input_id
PROXY_REGION  = "us"                         # 透传给 spider_inputs
MAX_CONCURRENCY_HINT = 100                   # 仅日志提示，实际并发在被调脚本内控制
# =================================

# 一组“筛选条件参数”（会被随机抽取），键名对齐 rep_02_03_filter.get_spider_url 读取的字段
FILTER_PARAMS: List[Dict[str, Any]] = [
    # 城市 + 在售 + 独栋 + 最近7天 + 最多抓 80 条（2 页 * 40）
    {"keywords-location": "South Bend IN", "listingCategory": "For Sale", "HomeType": "Houses", "days_on_zillow": 7, "maximum": 80},
    # 纽约：租房 + 公寓，近14天
    {"keywords-location": "New York NY", "listingCategory": "For Rent", "HomeType": "Apartments", "days_on_zillow": 14, "maximum": 80},
    # 洛杉矶：在售 + Condo/Co-op（HomeType 在源码里会映射并清空其它类型）
    {"keywords-location": "Los Angeles CA", "listingCategory": "For Sale", "HomeType": "Condos/Co-ops", "days_on_zillow": 30, "maximum": 40},
    # 迈阿密：在售 + Houses
    {"keywords-location": "Miami FL", "listingCategory": "For Sale", "HomeType": "Houses", "days_on_zillow": 30, "maximum": 40},
    # 华盛顿：租房 + Apartments
    {"keywords-location": "Washington DC", "listingCategory": "For Rent", "HomeType": "Apartments", "days_on_zillow": 14, "maximum": 80},
    # 芝加哥：在售 + Houses
    {"keywords-location": "Chicago IL", "listingCategory": "For Sale", "HomeType": "Houses", "days_on_zillow": 14, "maximum": 40},
]

def load_target_module(path: str):
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"找不到被测脚本：{abs_path}")
    spec = importlib.util.spec_from_file_location("target_module", abs_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod

def ensure_csv_path() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"zillow_filter_{ts}.csv")

def stringify_error(err_val: Any) -> str:
    """把 error 字段（set/list/tuple/str/None）转成可读字符串"""
    if err_val is None:
        return ""
    if isinstance(err_val, (set, list, tuple)):
        try:
            return "; ".join(sorted(map(str, err_val)))
        except Exception:
            return "; ".join(str(x) for x in err_val)
    return str(err_val)

async def build_spider_inputs(mod, user_parm: Dict[str, Any]) -> Dict[str, Any]:
    """
    参考 rep_02_03_filter.py main 的构造方式，自己生成 spider_inputs，
    避免 handle_parameter() 内对 get_spider_url 的同步调用问题。
    """
    # 正确地 await 获取搜索 URL
    url = await mod.get_spider_url(user_parm)

    # 和源码 main 一样的字段布局
    spider_inputs = {
        "user_input": user_parm.copy(),
        "spider_input": {
            "spider_errors": "True" if SPIDER_ERRORS else "False",
            "proxy_region": user_parm.get("proxy_region", PROXY_REGION),
            "url": url,
            "user_input_id": USER_INPUT_ID,
            "spider_item": {},       # rep_02_03_filter 内部会更新
            "spider_url": url,
            "maximum": int(user_parm.get("maximum", 0) or 0),
        },
        "discovery_input": {},
    }
    return spider_inputs

async def run_once(mod, user_parm: Dict[str, Any]) -> Dict[str, Any]:
    """
    运行一次：构造 spider_inputs -> 调 get_zillow_page_content -> 统计指标
    返回用于落表的一行 dict
    """
    spider_inputs = await build_spider_inputs(mod, user_parm)

    t0 = time.time()
    try:
        # 直接调用被调脚本里的抓取主流程（内部会：搜索页 -> 翻页 -> 提取每个房源详情）
        results = await mod.get_zillow_page_content(spider_inputs, cookies=None, impersonate=True, retry_times=5)
        elapsed = time.time() - t0
    except Exception as e:
        # 任何异常也落表
        return {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "location": user_parm.get("keywords-location", ""),
            "listingCategory": user_parm.get("listingCategory", ""),
            "HomeType": user_parm.get("HomeType", ""),
            "days_on_zillow": user_parm.get("days_on_zillow", ""),
            "maximum": user_parm.get("maximum", ""),
            "seed_url": "",  # 失败时可能没有 URL
            "elapsed_sec": round(0.0, 3),
            "result_num": 0,
            "size_in_bytes": 0,
            "error_number": 1,
            "error_rate": 0.0,
            "errors": f"EXC {type(e).__name__}: {e}",
        }

    # 统计指标（对齐 rep_02_03_filter.zillow_multiple_resp 的逻辑）
    result_num = 0
    size_in_bytes = 0
    error_number = 0
    error_bag = set()

    # results 是一个“详情结果”列表（被调脚本内部并发获取）
    for item in results:
        result_num += 1
        # 计算体积
        try:
            size_in_bytes += len(json.dumps(item, ensure_ascii=False).encode("utf-8"))
        except Exception:
            # 如果某个字段不可序列化，退化用 str
            size_in_bytes += len(str(item).encode("utf-8"))

        # 统计错误
        if not item.get("success", False):
            error_number += 1
        if item.get("error"):
            err = f'{item.get("error_code", 520)} {item.get("error")}'
            error_bag.add(err)

    error_rate = round((error_number / result_num), 4) if result_num else 0.0

    row = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "location": user_parm.get("keywords-location", ""),
        "listingCategory": user_parm.get("listingCategory", ""),
        "HomeType": user_parm.get("HomeType", ""),
        "days_on_zillow": user_parm.get("days_on_zillow", ""),
        "maximum": user_parm.get("maximum", ""),
        "seed_url": spider_inputs["spider_input"]["spider_url"],
        "elapsed_sec": round(elapsed, 3),
        "result_num": result_num,
        "size_in_bytes": size_in_bytes,
        "error_number": error_number,
        "error_rate": error_rate,
        "errors": "; ".join(sorted(map(str, error_bag))) if error_bag else "",
    }
    return row

async def main_async():
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)

    mod = load_target_module(TARGET_PATH)

    # 即时落盘：先写表头
    csv_path = ensure_csv_path()
    header = [
        "ts","location","listingCategory","HomeType","days_on_zillow","maximum",
        "seed_url","elapsed_sec","result_num","size_in_bytes","error_number","error_rate","errors"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
    written = 0

    for run_idx in range(1, TOTAL_RUNS + 1):
        user_parm = random.choice(FILTER_PARAMS)
        row = await run_once(mod, user_parm)

        # 逐行即时写入
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writerow(row)
            f.flush()
        written += 1

        # 控制台提示
        print(f"[{run_idx}/{TOTAL_RUNS}] {row['location']} -> result_num={row['result_num']} "
              f"errors={row['error_number']}/{row['result_num']} rate={row['error_rate']} "
              f"elapsed={row['elapsed_sec']}s")

    print("✅ 运行完成")
    print(f"  总运行次数 : {TOTAL_RUNS}")
    print(f"  已写入行数 : {written}")
    print(f"  并发提示   : 内部详情抓取上限 {MAX_CONCURRENCY_HINT}（具体以内置实现为准）")
    print(f"  CSV路径    : {os.path.abspath(csv_path)}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
