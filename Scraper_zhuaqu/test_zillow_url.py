# -*- coding: utf-8 -*-
"""
Zillow 详情页测试（更低层：直接调用 zillow_multiple_resp）
- 入口：zillow_multiple_resp(user_inputs, cos_key, error, retry_times)
  返回 5 元组: (result_num, total_time, size_in_bytes, error_number, error)
- TOTAL_RUNS 控制总运行次数；每次随机 1 个 URL
- CSV 不写汇总行；逐字段落表；error 可读化（绝不 json.dumps 整个返回元组）
"""

import os
import csv
import random
import asyncio
import importlib.util
from datetime import datetime
from typing import Any, Dict, List, Tuple, Iterable

# ============= 可配置 =============
TARGET_PATH   = "./rep_02_01_detail.py"   # 被测脚本路径
TOTAL_RUNS    = 100                          # ★总运行次数（每次随机 1 个 URL）
RETRY_TIMES   = 5                          # 被调函数内部重试
OUTPUT_DIR    = "./zillow_detail_results"  # 输出目录
RANDOM_SEED   = 42                         # 设 None 则每次随机不同

ZILLOW_URLS   = [
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
    "https://www.zillow.com/homedetails/201-Saint-Paul-St-Baltimore-MD-21202/3658164_zpid/"
]
# =================================

def load_target_module(path: str):
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"找不到被测脚本：{abs_path}")
    spec = importlib.util.spec_from_file_location("target_module", abs_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod

def build_user_inputs(spider_id: str, url: str, spider_errors: bool = False) -> Dict[str, Any]:
    # 与 rep_02_01_detail.handle_parameter() 的入参结构一致
    return {
        "spider_id": spider_id,
        "spider_parameters": [{"url": url}],
        "spider_errors": "True" if spider_errors else "False",
    }

def ensure_csv_path() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"zillow_detail_resp_{ts}.csv")

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

async def call_once_resp(mod, url: str, run_idx: int) -> Tuple[int, float, int, int, Any, float, List[str]]:
    """
    直接调用更低层的 zillow_multiple_resp()
    返回:
      result_num, total_time, size_in_bytes, error_number, error
    以及额外返回: elapsed(总耗时), outer_error_list（外层 error set）
    """
    user_inputs = build_user_inputs("zillow_detail", url, spider_errors=False)
    print("user_inputs", user_inputs)
    cos_key = f"zillow_detail:{datetime.now().strftime('%Y%m%d_%H%M%S')}:{run_idx}"
    error_set = set()

    # 计时开始
    t0 = datetime.now().timestamp()
    # 调用底层函数
    result_tuple = await mod.zillow_multiple_resp(
        user_inputs=user_inputs,
        cos_key=cos_key,
        error=error_set,
        retry_times=RETRY_TIMES,
    )
    elapsed = datetime.now().timestamp() - t0

    # 兼容处理：确保可以安全解包
    result_num = total_time = size_in_bytes = error_number = None
    error_obj = None
    if isinstance(result_tuple, tuple) and len(result_tuple) >= 5:
        result_num, total_time, size_in_bytes, error_number, error_obj = result_tuple[:5]
    else:
        # 非预期返回，尽量落表
        error_obj = f"UNEXPECTED_RETURN: {type(result_tuple).__name__} len={getattr(result_tuple, '__len__', lambda: 'NA')()}"
    return (
        result_num or 0,
        float(total_time or 0),
        int(size_in_bytes or 0),
        int(error_number or 0),
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
        "ts","url","run_idx","elapsed_sec",
        "result_num","total_time","size_in_bytes","error_number","error_text",
        "error_rate","outer_error_set"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
    written = 0

    for run_idx in range(1, TOTAL_RUNS + 1):
        url = random.choice(ZILLOW_URLS)
        try:
            result_num, total_time, size_in_bytes, error_number, error_obj, elapsed, err_list = \
                await call_once_resp(mod, url, run_idx)
        except Exception as e:
            # 任何异常都落表，不中断
            result_num = 0
            total_time = 0.0
            size_in_bytes = 0
            error_number = 1
            error_obj = f"EXC {type(e).__name__}: {e}"
            elapsed = 0.0
            err_list = [error_obj]

        # 计算 error_rate（避免除零）
        if result_num and result_num > 0:
            error_rate = round((error_number or 0) / result_num, 4)
        else:
            error_rate = 0.0

        # 写入一行
        row = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "url": url,
            "run_idx": run_idx,
            "elapsed_sec": round(elapsed, 3),

            # —— 来自 zillow_multiple_resp 的 5 字段 —— #
            "result_num": result_num,
            "total_time": total_time,
            "size_in_bytes": size_in_bytes,
            "error_number": error_number,
            "error_text": stringify_error(error_obj),  # 可读化

            # —— 衍生指标 —— #
            "error_rate": error_rate,

            # —— 外层累积 —— #
            "outer_error_set": "; ".join(err_list) if err_list else "",
        }
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writerow(row)
            f.flush()  # 立即刷新，防止异常退出丢数据
        written += 1



    print("✅ 运行完成")
    print(f"  总运行次数 : {TOTAL_RUNS}")
    print(f"  CSV路径    : {os.path.abspath(csv_path)}")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
