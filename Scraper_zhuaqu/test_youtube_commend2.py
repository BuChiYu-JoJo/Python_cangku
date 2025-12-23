# -*- coding: utf-8 -*-
"""
YouTube 评论抓取测试（参考 rep_01_02_commend.py）
- 入口：youtube_multiple_results_commend(user_inputs, cos_key, error, retry_times)
  期望返回 7 元组：
    (total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error)
- TOTAL_RUNS 控制总运行次数；每次随机 1 个 Video ID
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
TARGET_PATH      = "./rep_01_02_commend.py"       # 被测脚本路径
TOTAL_RUNS       = 5                              # ★总运行次数（每次随机 1 个 Video ID）
RETRY_TIMES      = 5                               # 被调函数内部重试
OUTPUT_DIR       = "./youtube_comment_results"     # 输出目录
RANDOM_SEED      = 42                              # 设 None 则每次随机不同
PAGE_TURNING     = 2                               # 评论翻页次数（>=1）
LOAD_REPLIES     = 0                               # 是否加载子回复：0=否，1=是
PROXY_REGION     = "us"                            # 代理区域
SPIDER_ERRORS    = False                           # 是否将结果标注 err_stats（传 "True"/"False"）

# YouTube 测试 Video ID（可根据需要增减/替换）
YOUTUBE_VIDEO_IDS = [
    "dQw4w9WgXcQ",  # Rick Astley - Never Gonna Give You Up
    "kJQP7kiw5Fk",  # Luis Fonsi - Despacito ft. Daddy Yankee
    "9bZkp7q19f0",  # PSY - GANGNAM STYLE
    "JGwWNGJdvx8",  # Ed Sheeran - Shape of You
    "fRh_vgS2dFE",  # Justin Bieber - Sorry
    "OPf0YbXqDm0",  # Mark Ronson - Uptown Funk ft. Bruno Mars
    "CevxZvSJLk8",  # Katy Perry - Roar
    "hT_nvWreIhg",  # OneRepublic - Counting Stars
    "e-ORhEE9VVg",  # Taylor Swift - Blank Space
    "2Vv-BfVoq4g",  # Ed Sheeran - Perfect
    "YQHsXMglC9A",  # Adele - Hello
    "uelHwf8o7_U",  # Eminem - Love The Way You Lie ft. Rihanna
    "pRpeEdMmmQ0",  # Shakira - Waka Waka
    "oC-GflRB0y4",  # NewJeans (示例：如遇区域限制可更换)
    "K5KAc5CoCuk",  # LMFAO - Party Rock Anthem
    "RgKAFK5djSk",  # Wiz Khalifa - See You Again
    "ktvTqknDobU",  # Imagine Dragons - Radioactive
    "mWRsgZuwf_8",  # Imagine Dragons - Demons
    "hTWKbfoikeg",  # Nirvana - Smells Like Teen Spirit
    "3JZ_D3ELwOQ",  # Adele - Rolling in the Deep
]
# ================================== #


def load_target_module(path: str):
    """动态加载被测模块 rep_01_02_commend.py"""
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"找不到被测脚本：{abs_path}")
    spec = importlib.util.spec_from_file_location("target_module", abs_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def build_user_inputs(
    spider_id: str,
    video_id: str,
    domain: str = "",
    page_turning: int = PAGE_TURNING,
    load_replies: int = LOAD_REPLIES,
    proxy_region: str = PROXY_REGION,
    spider_errors: bool = SPIDER_ERRORS,
) -> Dict[str, Any]:
    """
    构造与 rep_01_02_commend.handle_parameter() 匹配的入参：
      {
        "spider_id": str,
        "spider_info": [{
            "video_id": str, "domain": str, "page_turning": int,
            "load_replies": int, "proxy_region": "us"
        }],
        "spider_errors": "True"/"False"
      }
    """
    return {
        "spider_id": spider_id,
        "spider_info": [{
            "video_id": video_id,
            "domain": domain,
            "page_turning": page_turning,
            "load_replies": load_replies,
            "proxy_region": proxy_region,
        }],
        "spider_errors": "True" if spider_errors else "False",
    }


def ensure_csv_path() -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(OUTPUT_DIR, f"youtube_comment_resp_{ts}.csv")


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


async def call_once(mod, video_id: str, run_idx: int) -> Tuple[int, int, float, float, int, int, Any, float, List[str], str]:
    """
    调用更高层的 youtube_multiple_results_commend():
      返回 7 元组: (total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error)
    附加返回：elapsed(总耗时秒)、outer_error_list（外层 error set）、url（用于落表）
    """
    user_inputs = build_user_inputs(
        spider_id="yt_comment",
        video_id=video_id,
        domain="",  # 为空则默认 https://www.youtube.com
    )
    cos_key = f"youtube_comment:{datetime.now().strftime('%Y%m%d_%H%M%S')}:{run_idx}"
    error_set = set()

    t0 = datetime.now().timestamp()
    result_tuple = await mod.youtube_multiple_results_commend(
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

    url = f"https://www.youtube.com/watch?v={video_id}"
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
        url,
    )


async def main_async():
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)

    mod = load_target_module(TARGET_PATH)

    csv_path = ensure_csv_path()
    header = [
        "ts", "video_id", "url", "run_idx", "elapsed_sec",
        # —— 来自 youtube_multiple_results_commend 的 7 字段 —— #
        "total_number", "size_in_bytes", "total_time", "error_rate", "error_number", "len_result", "error_text",
        # —— 外层累积 —— #
        "outer_error_set",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()

    for run_idx in range(1, TOTAL_RUNS + 1):
        vid = random.choice(YOUTUBE_VIDEO_IDS)
        try:
            (total_number, size_in_bytes, total_time, error_rate,
             error_number, len_result, error_obj, elapsed, err_list, url) = await call_once(mod, vid, run_idx)
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
            url = f"https://www.youtube.com/watch?v={vid}"

        row = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "video_id": vid,
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
