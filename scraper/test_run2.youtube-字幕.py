# -*- coding: utf-8 -*-
"""
极简测试脚本：
- 动态加载被测脚本（默认 ./rep_01_05_download_subtitle.py）
- 多次调用 youtube_multiple_results_download_subtitle(...)
- 每次写入必要字段到CSV，并打印汇总（成功率/耗时/字节）
- 测试侧做“键名兜底 + 猴子补丁”以避免 KeyError('subtitles_type')
"""

import os
import sys
import csv
import time
import json
import argparse
import importlib.util
import asyncio
from datetime import datetime

# ======== 可按需改的默认参数 ========
DEFAULT_TARGET = "./rep_01_05_download_subtitle.py"
DEFAULT_RUNS = 3
DEFAULT_DELAY = 1.0
DEFAULT_RETRY_TIMES = 3
DEFAULT_OUTPUT_CSV = "./test_runs_summary.csv"

DEFAULT_VIDEO_ID = "LHCob76kigA"
DEFAULT_DOMAIN = "https://www.youtube.com"
DEFAULT_PROXY_REGION = "us"
DEFAULT_LANG = "zh"
DEFAULT_SUBTYPE = "auto_generated"
DEFAULT_PROXY = ""  # 如需代理： http://user:pwd@host:port
# ==================================

def load_target_module(path: str):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到被测脚本：{path}")
    spec = importlib.util.spec_from_file_location("target_module", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod

def _normalize_si(si: dict) -> dict:
    """对单个 spider_input/spider_info dict 做键名兜底，确保 subtitles_type 存在。"""
    # 读 alias
    st = (
        si.get("subtitles_type")
        or si.get("subtitle_type")
        or si.get("subtitlesType")
        or si.get("subtitleType")
    )
    if not st:
        st = "auto_generated"
    # 写回所有可能键位
    si["subtitles_type"] = st
    si["subtitle_type"] = st
    si["subtitlesType"] = st
    si["subtitleType"] = st

    lang = si.get("subtitles_language") or si.get("language") or si.get("lang") or "zh"
    si["subtitles_language"] = lang
    si["language"] = lang
    si["lang"] = lang
    return si

def build_user_inputs(video_id: str,
                      domain: str,
                      proxy_region: str,
                      lang: str,
                      subtype: str,
                      proxy: str,
                      runs: int) -> dict:
    base = {
        "video_id": video_id,
        "domain": domain,
        "proxy_region": proxy_region,
        "subtitles_language": lang,
        "subtitles_type": subtype,
        # 一次性把所有别名也塞进去，最大化兼容
        "subtitle_type": subtype,
        "subtitlesType": subtype,
        "subtitleType": subtype,
        "language": lang,
        "lang": lang,
    }
    return {
        "spider_id": "youtube_subtitle_download",
        "spider_errors": True,
        "proxy": proxy or "",
        "spider_info": [dict(base)],
        "spider_input": dict(base),
        "_runs": runs,
    }

def make_cos_key() -> str:
    now = datetime.now()
    return f"scrapers/thordata/{now:%Y/%m/%d}/task_{now:%Y%m%d%H%M%S}_{os.getpid()}"

def ensure_csv(path: str):
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts", "video_id", "lang", "type",
                "total_number", "len_result", "error_number",
                "error_rate", "size_in_bytes", "total_time", "success"
            ])

def append_csv(path: str, row: dict):
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row["ts"],
            row["video_id"],
            row["lang"],
            row["type"],
            row["total_number"],
            row["len_result"],
            row["error_number"],
            row["error_rate"],
            row["size_in_bytes"],
            row["total_time"],
            row["success"],
        ])

def monkey_patch_module(mod):
    """
    给被测模块打猴子补丁：
    - 在 get_youtube_page_content 调用前确保 spider_inputs['spider_input']['subtitles_type'] 存在。
    - 可选：对 youtube_multiple_resp 做同样兜底（内部可能也重建/改写）。
    """
    # patch get_youtube_page_content
    if hasattr(mod, "get_youtube_page_content"):
        orig = mod.get_youtube_page_content
        async def patched(spider_inputs, *args, **kwargs):
            # 兜底 spider_input
            si = spider_inputs.get("spider_input") or {}
            spider_inputs["spider_input"] = _normalize_si(dict(si))
            # 兜底 spider_info[0]
            if "spider_info" in spider_inputs and isinstance(spider_inputs["spider_info"], list) and spider_inputs["spider_info"]:
                spider_inputs["spider_info"][0] = _normalize_si(dict(spider_inputs["spider_info"][0]))
            return await orig(spider_inputs, *args, **kwargs)
        mod.get_youtube_page_content = patched

    # 可选：patch youtube_multiple_resp（谨慎覆盖）
    if hasattr(mod, "youtube_multiple_resp"):
        orig2 = mod.youtube_multiple_resp
        async def patched2(user_inputs, *args, **kwargs):
            # 顶层也兜底
            if "spider_input" in user_inputs:
                user_inputs["spider_input"] = _normalize_si(dict(user_inputs["spider_input"]))
            if "spider_info" in user_inputs and isinstance(user_inputs["spider_info"], list) and user_inputs["spider_info"]:
                user_inputs["spider_info"][0] = _normalize_si(dict(user_inputs["spider_info"][0]))
            return await orig2(user_inputs, *args, **kwargs)
        mod.youtube_multiple_resp = patched2

async def call_once(mod, user_inputs: dict, retry_times: int = 3) -> dict:
    """
    调一次被测函数：
      youtube_multiple_results_download_subtitle(user_inputs, cos_key, error, retry_times)
    返回测试侧最小字段。
    """
    # 运行前也做一次兜底，确保入参包含字段
    user_inputs["spider_input"] = _normalize_si(dict(user_inputs["spider_input"]))
    if isinstance(user_inputs.get("spider_info"), list) and user_inputs["spider_info"]:
        user_inputs["spider_info"][0] = _normalize_si(dict(user_inputs["spider_info"][0]))

    cos_key = make_cos_key()
    error_set = set()

    # 核心调用（被测模块需具备该异步函数）
    total_number, size_in_bytes, total_time, error_rate, error_number, \
        len_result, error, video_link = await mod.youtube_multiple_results_download_subtitle(
            user_inputs, cos_key, error_set, retry_times=retry_times
        )

    return {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "video_id": user_inputs["spider_input"]["video_id"],
        "lang": user_inputs["spider_input"]["subtitles_language"],
        "type": user_inputs["spider_input"]["subtitles_type"],
        "total_number": total_number,
        "len_result": len_result,
        "error_number": error_number,
        "error_rate": error_rate,
        "size_in_bytes": int(size_in_bytes),
        "total_time": round(float(total_time), 3),
        "success": 1 if (error_number == 0 and len_result > 0) else 0,
    }

async def main_async(args):
    mod = load_target_module(args.target)
    # 打补丁，防止被测内部漏字段导致 KeyError
    monkey_patch_module(mod)

    user_inputs = build_user_inputs(
        video_id=args.video_id,
        domain=args.domain,
        proxy_region=args.proxy_region,
        lang=args.lang,
        subtype=args.subtype,
        proxy=args.proxy,
        runs=args.runs,
    )

    ensure_csv(args.output_csv)

    rows = []
    for i in range(args.runs):
        print(f"==== Test Run {i+1}/{args.runs} ====")
        # 打印一次核心入参（供定位）
        print("user_inputs", json.dumps({
            "spider_id": user_inputs["spider_id"],
            "spider_errors": user_inputs["spider_errors"],
            "proxy": user_inputs["proxy"],
            "spider_input": user_inputs["spider_input"],
        }, ensure_ascii=False))
        row = await call_once(mod, user_inputs, retry_times=args.retry_times)
        append_csv(args.output_csv, row)
        rows.append(row)
        if i < args.runs - 1 and args.delay > 0:
            time.sleep(args.delay)

    # 汇总
    total = len(rows)
    succ = sum(r["success"] for r in rows)
    avg_time = round(sum(r["total_time"] for r in rows) / max(total, 1), 3)
    total_bytes = sum(r["size_in_bytes"] for r in rows)
    print("\nSummary:")
    print(f"  runs           : {total}")
    print(f"  success_runs   : {succ}")
    print(f"  success_rate   : {round(succ / total, 2) if total else 0.0}")
    print(f"  avg_total_time : {avg_time}")
    print(f"  total_bytes    : {total_bytes}")
    print(f"  CSV saved to   : {os.path.abspath(args.output_csv)}")

def parse_args():
    p = argparse.ArgumentParser(description="Minimal test harness for YouTube subtitles spider")
    p.add_argument("--target", default=DEFAULT_TARGET, help="被测脚本路径")
    p.add_argument("--runs", type=int, default=DEFAULT_RUNS, help="循环运行次数")
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="每次运行间隔秒")
    p.add_argument("--retry-times", type=int, default=DEFAULT_RETRY_TIMES, help="传给被测函数的重试次数")
    p.add_argument("--output-csv", default=DEFAULT_OUTPUT_CSV, help="CSV 输出路径")
    p.add_argument("--video-id", default=DEFAULT_VIDEO_ID, help="YouTube 视频ID")
    p.add_argument("--domain", default=DEFAULT_DOMAIN, help="域名")
    p.add_argument("--proxy-region", default=DEFAULT_PROXY_REGION, help="代理区域")
    p.add_argument("--lang", default=DEFAULT_LANG, help="字幕语言（如 zh/en）")
    p.add_argument("--subtype", default=DEFAULT_SUBTYPE, help="字幕类型：auto_generated/manual/all")
    p.add_argument("--proxy", default=DEFAULT_PROXY, help="代理（可空；http://user:pwd@host:port）")
    return p.parse_args()

def main():
    args = parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
