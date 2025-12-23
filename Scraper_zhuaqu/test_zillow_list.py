# -*- coding: utf-8 -*-
"""
Zillow 列表页测试（参考 rep_02_02_list.py）
- 入口：zillow_multiple_results_list(user_inputs, cos_key, error, retry_times)
  期望返回 7 元组：
    (total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error)
- TOTAL_RUNS 控制总运行次数；每次随机 1 个列表页 URL
- CSV 实时写入；将 error/readable 化，避免整对象直接 dump
"""

import os
import csv
import random
import asyncio
import importlib.util
from datetime import datetime
from typing import Any, Dict, List, Tuple

# ============= 可配置 ============= #
TARGET_PATH   = "./rep_02_02_list.py"     # 被测脚本路径（已改为列表页脚本）
TOTAL_RUNS    = 3                       # ★总运行次数（每次随机 1 个 URL）
RETRY_TIMES   = 5                         # 被调函数内部重试
OUTPUT_DIR    = "./zillow_list_results"   # 输出目录
RANDOM_SEED   = 42                        # 设 None 则每次随机不同
DEFAULT_MAXIMUM = 10                      # 列表页最大抓取条数（传给 spider_parameters.maximum）

# 适配列表页的 Zillow 搜索 URL（可按需增减）
ZILLOW_LIST_URLS = [
    # 来自 rep_02_02_list.py 示例
    "https://www.zillow.com/south-bend-in/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-86.28027395481374%2C%22east%22%3A-86.21281103367116%2C%22south%22%3A41.6786743836223%2C%22north%22%3A41.717895122883554%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A20555%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isEntirePlaceForRent%22%3Atrue%2C%22isRoomForRent%22%3Afalse%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A14%7D",

    # 其它示例（可替换为你常测城市/区域）
    "https://www.zillow.com/new-york-ny/",
    "https://www.zillow.com/los-angeles-ca/",
    "https://www.zillow.com/chicago-il/",
    "https://www.zillow.com/miami-fl/",
]
# ================================== #


def load_target_module(path: str):
    """动态加载被测模块 rep_02_02_list.py"""
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"找不到被测脚本：{abs_path}")
    spec = importlib.util.spec_from_file_location("target_module", abs_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def build_user_inputs(spider_id: str, url: str, maximum: int = DEFAULT_MAXIMUM, spider_errors: bool = False) -> Dict[str, Any]:
    """
    构造与 rep_02_02_list.handle_parameter() 匹配的入参：
      {
        "spider_id": str,
        "spider_parameters": [{"url": str, "maximum": int}],
        "spider_errors": "True"/"False"
      }
    """
    return {
        "spider_id": spider_id,
        "spider_parameters": [{"url": url, "maximum": maximum}],
        "spider_errors": "True" if spider_errors else "False",
    }


def ensure_csv_path() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"zillow_list_resp_{ts}.csv")


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


async def call_once_results(mod, url: str, run_idx: int) -> Tuple[int, int, float, float, int, int, Any, float, List[str]]:
    """
    调用更高层的 zillow_multiple_results_list():
      返回 7 元组: (total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error)
    附加返回：elapsed(总耗时秒)、outer_error_list（外层 error set）
    """
    user_inputs = build_user_inputs("zillow_list", url, maximum=DEFAULT_MAXIMUM, spider_errors=False)
    cos_key = f"zillow_list:{datetime.now().strftime('%Y%m%d_%H%M%S')}:{run_idx}"
    error_set = set()

    t0 = datetime.now().timestamp()
    result_tuple = await mod.zillow_multiple_results_list(
        user_inputs=user_inputs,
        cos_key=cos_key,
        error=error_set,
        retry_times=RETRY_TIMES,
    )
    elapsed = datetime.now().timestamp() - t0

    # 解包并兜底
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
        # —— 来自 zillow_multiple_results_list 的 7 字段 —— #
        "total_number", "size_in_bytes", "total_time", "error_rate", "error_number", "len_result", "error_text",
        # —— 外层累积 —— #
        "outer_error_set",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()

    written = 0
    for run_idx in range(1, TOTAL_RUNS + 1):
        url = random.choice(ZILLOW_LIST_URLS)
        try:
            (total_number, size_in_bytes, total_time, error_rate,
             error_number, len_result, error_obj, elapsed, err_list) = await call_once_results(mod, url, run_idx)
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
        written += 1

    print("✅ 运行完成")
    print(f"  总运行次数 : {TOTAL_RUNS}")
    print(f"  CSV路径    : {os.path.abspath(csv_path)}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
