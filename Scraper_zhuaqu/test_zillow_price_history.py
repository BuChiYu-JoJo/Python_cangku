# -*- coding: utf-8 -*-
"""
Zillow 价格历史测试（参考 rep_02_04_price_history.py）
- 入口：zillow_multiple_results_price_history(user_inputs, cos_key, error, retry_times)
  期望返回 7 元组：
    (total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error)
- TOTAL_RUNS 控制总运行次数；每次随机 1 个 Zillow 详情页 URL
- CSV 实时写入；将 error/readable 化，避免将集合/对象直接写入
"""

import os
import csv
import random
import asyncio
import importlib.util
from datetime import datetime
from typing import Any, Dict, List, Tuple

# ============= 可配置 ============= #
TARGET_PATH      = "./rep_02_04_price_history.py"  # 被测脚本路径
TOTAL_RUNS       = 5                               # ★总运行次数（每次随机 1 个 URL）
RETRY_TIMES      = 5                                # 被调函数内部重试
OUTPUT_DIR       = "./zillow_price_history_results" # 输出目录
RANDOM_SEED      = 42                               # 设 None 则每次随机不同

# Zillow 详情页 URL（可按需增减/替换）
ZILLOW_DETAIL_URLS = [
    "https://www.zillow.com/homedetails/1060-North-Ave-Burlington-VT-05408/75437497_zpid/",
    "https://www.zillow.com/homedetails/2506-Gordon-Cir-South-Bend-IN-46635/77050198_zpid/?t=for_sale",
    "https://www.zillow.com/homedetails/1310-S-10th-St-Saint-Joseph-MO-64503/41004718_zpid/",
    "https://www.zillow.com/homedetails/223-W-80th-St-Apt-3B-New-York-NY-10024/244871666_zpid/",
    "https://www.zillow.com/homedetails/1550-N-Lake-Shore-Dr-APT-23W-Chicago-IL-60610/2071470903_zpid/",
    "https://www.zillow.com/homedetails/730-14th-St-Santa-Monica-CA-90402/20593336_zpid/",
    "https://www.zillow.com/homedetails/215-E-68th-St-APT-12C-New-York-NY-10065/244870527_zpid/",
    "https://www.zillow.com/homedetails/1575-W-29th-St-Los-Angeles-CA-90007/20053469_zpid/",
    "https://www.zillow.com/homedetails/401-E-60th-St-APT-5D-New-York-NY-10022/31594263_zpid/",
    "https://www.zillow.com/homedetails/10201-Grosvenor-Pl-APT-1115-Rockville-MD-20852/37210681_zpid/",
    "https://www.zillow.com/homedetails/1615-Q-St-NW-APT-505-Washington-DC-20009/441727_zpid/",
    "https://www.zillow.com/homedetails/4826-5th-St-NW-Washington-DC-20011/438043_zpid/",
    "https://www.zillow.com/homedetails/500-Alton-Rd-APT-1406-Miami-Beach-FL-33139/2061601539_zpid/",
    "https://www.zillow.com/homedetails/1500-Bay-Rd-APT-1108S-Miami-Beach-FL-33139/44268163_zpid/",
    "https://www.zillow.com/homedetails/1720-19th-St-S-Saint-Petersburg-FL-33712/45043268_zpid/",
    "https://www.zillow.com/homedetails/10355-Wilshire-Blvd-APT-5D-Los-Angeles-CA-90024/2071443764_zpid/",
    "https://www.zillow.com/homedetails/21200-Point-Pl-APT-2801-Aventura-FL-33180/43996305_zpid/",
    "https://www.zillow.com/homedetails/181-E-65th-St-APT-11B-New-York-NY-10065/72532188_zpid/",
    "https://www.zillow.com/homedetails/40-Broad-St-APT-20C-New-York-NY-10004/72529916_zpid/",
    "https://www.zillow.com/homedetails/201-Saint-Paul-St-Baltimore-MD-21202/3658164_zpid/",
]
# ================================== #


def load_target_module(path: str):
    """动态加载被测模块 rep_02_04_price_history.py"""
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"找不到被测脚本：{abs_path}")
    spec = importlib.util.spec_from_file_location("target_module", abs_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def build_user_inputs(spider_id: str, url: str, spider_errors: bool = False) -> Dict[str, Any]:
    """
    构造与 rep_02_04_price_history.handle_parameter() 匹配的入参：
      {
        "spider_id": str,
        "spider_parameters": [{"url": str}],
        "spider_errors": "True"/"False"
      }
    """
    return {
        "spider_id": spider_id,
        "spider_parameters": [{"url": url}],
        "spider_errors": "True" if spider_errors else "False",
    }


def ensure_csv_path() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"zillow_price_history_resp_{ts}.csv")


def stringify_error(err_val: Any) -> str:
    """把 error 字段（可能是 set/list/tuple/str/None）转成可读字符串"""
    if err_val is None:
        return ""
    if isinstance(err_val, (set, list, tuple)):
        try:
            return "; ".join(sorted(map(str, err_val)))
        except Exception:
            return "; ".join(str(x) for x in err_val)
    return str(err_val)


async def call_once(mod, url: str, run_idx: int) -> Tuple[int, int, float, float, int, int, Any, float, List[str]]:
    """
    调用更高层的 zillow_multiple_results_price_history():
      返回 7 元组: (total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error)
    附加返回：elapsed(总耗时秒)、outer_error_list（外层 error set）
    """
    user_inputs = build_user_inputs("zillow_price_history", url, spider_errors=False)
    cos_key = f"zillow_price_history:{datetime.now().strftime('%Y%m%d_%H%M%S')}:{run_idx}"
    error_set = set()

    t0 = datetime.now().timestamp()
    result_tuple = await mod.zillow_multiple_results_price_history(
        user_inputs=user_inputs,
        cos_key=cos_key,
        error=error_set,
        retry_times=RETRY_TIMES,
    )
    elapsed = datetime.now().timestamp() - t0

    # 安全解包
    total_number = size_in_bytes = error_number = len_result = 0
    total_time = error_rate = 0.0
    error_obj: Any = None

    if isinstance(result_tuple, tuple) and len(result_tuple) >= 7:
        total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error_obj = result_tuple[:7]
    else:
        error_obj = f"UNEXPECTED_RETURN: {type(result_tuple).__name__} len={getattr(result_tuple, '__len__', lambda: 'NA')()}"

    return (
        int(total_number or 0),
        int(size_in_bytes or 0),
        float(total_time or 0.0),
        float(error_rate or 0.0),
        int(error_number or 0),
        int(len_result or 0),
        error_obj,
        float(elapsed),
        sorted(error_set),
    )


async def main_async():
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)

    mod = load_target_module(TARGET_PATH)

    csv_path = ensure_csv_path()
    header = [
        "ts", "url", "run_idx", "elapsed_sec",
        # —— 来自 zillow_multiple_results_price_history 的 7 字段 —— #
        "total_number", "size_in_bytes", "total_time", "error_rate", "error_number", "len_result", "error_text",
        # —— 外层累积 —— #
        "outer_error_set",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()

    for run_idx in range(1, TOTAL_RUNS + 1):
        url = random.choice(ZILLOW_DETAIL_URLS)
        try:
            (total_number, size_in_bytes, total_time, error_rate,
             error_number, len_result, error_obj, elapsed, err_list) = await call_once(mod, url, run_idx)
        except Exception as e:
            # 任何异常都落表，不中断
            total_number = 0
            size_in_bytes = 0
            total_time = 0.0
            error_rate = 1.0
            error_number = 1
            len_result = 0
            error_obj = f"EXC {type(e).__name__}: {e}"
            elapsed = 0.0
            err_list = [error_obj]

        row = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "url": url,
            "run_idx": run_idx,
            "elapsed_sec": round(elapsed, 3),

            # —— 7 字段 —— #
            "total_number": total_number,
            "size_in_bytes": size_in_bytes,
            "total_time": round(total_time, 3),
            "error_rate": round(error_rate, 4),
            "error_number": error_number,
            "len_result": len_result,
            "error_text": stringify_error(error_obj),

            # —— 外层 error —— #
            "outer_error_set": "; ".join(err_list) if err_list else "",
        }
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writerow(row)
            f.flush()

    print("✅ 运行完成")
    print(f"  总运行次数 : {TOTAL_RUNS}")
    print(f"  CSV路径    : {os.path.abspath(csv_path)}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
