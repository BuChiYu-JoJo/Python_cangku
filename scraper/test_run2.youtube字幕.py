# -*- coding: utf-8 -*-
"""
YouTube 字幕下载 Runner（健壮版 + 多次运行配置）
- 解决 KeyError: 'subtitles_type' 等输入不一致问题
- 兼容 spider_info 为 list[dict] 或 dict
- 统一字段命名、值规范化（subtitles_type / language）
- 标准化任务统计与 CSV 写入
- 新增：多次运行、下载重试、假数据模式、清理旧文件
"""

import csv
import json
import logging
import os
import sys
import re
import glob
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, List

# =========================
# 全局基础配置（可按需修改）
# =========================

# CSV 保存路径（可按你的项目目录改）
CSV_PATH = "/home/thor-scraper-backend/projects/scraper_01_youtube/youtube_subtitles_runs.csv"

# COS Key 前缀（与现有统计兼容）
COS_PREFIX = "scrapers/thordata"

# 下载输出目录（字幕文件会保存到这里）
OUTPUT_DIR = os.path.abspath("./youtube_subs")

# —— 多次运行与行为开关 ——
RUN_TIMES = 3                 # 默认跑几次（可被 user_inputs["runs"] 覆盖）
RUN_DELAY_SECONDS = 1.0       # 每次运行之间的间隔秒
MAX_RETRIES = 2               # 下载重试次数（失败后再试多少次）
ENABLE_FAKE_MODE = False      # True=不真实下载，仅返回假数据，便于联调
CLEAR_OLD_FILES = True        # 下载前清理该 video_id 的旧字幕文件

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("youtube_subtitle_runner")


# =========================
# 规范化 & 校验
# =========================

# 可接受的字幕类型映射（别名->规范值）
ALLOWED_SUB_TYPES = {
    "auto_generated": "auto_generated",
    "auto-generated": "auto_generated",
    "auto": "auto_generated",
    "manual": "manual",
    "human": "manual",
    "all": "all",
}

# 语言别名映射（按你的业务需求补充）
LANG_ALIASES = {
    "zh": "zh",
    "zh-cn": "zh",
    "zh_cn": "zh",
    "zh-hans": "zh",
    "cn": "zh",
    "en": "en",
    "en-us": "en",
}

# 入参键名别名映射（各种常见写法统一到蛇形命名）
KEY_ALIASES = {
    "videoid": "video_id",
    "vid": "video_id",
    "subtitle_type": "subtitles_type",      # 常见少写 s
    "subtitlesType": "subtitles_type",      # 驼峰
    "subtitleType": "subtitles_type",       # 驼峰+少 s
    "language": "subtitles_language",
    "lang": "subtitles_language",
    "proxyRegion": "proxy_region",
    "domain_url": "domain",
}


def _normalize_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    """统一键名：别名映射 + 小写"""
    out = {}
    for k, v in d.items():
        kk = KEY_ALIASES.get(k, k)
        kk = kk.strip().lower()
        out[kk] = v
    return out


def _coerce_subtitles_type(v: Optional[str]) -> str:
    """规范化字幕类型，兜底为 auto_generated"""
    if v is None:
        return "auto_generated"
    vv = str(v).strip().lower().replace(" ", "_")
    return ALLOWED_SUB_TYPES.get(vv, "auto_generated")


def _coerce_language(v: Optional[str]) -> str:
    """规范化语言代码，默认 zh"""
    if v is None:
        return "zh"
    vv = str(v).strip().lower()
    return LANG_ALIASES.get(vv, vv)


@dataclass
class YouTubeSubtitleConfig:
    video_id: str
    domain: str = "https://www.youtube.com"
    proxy_region: Optional[str] = None
    subtitles_language: str = "zh"
    subtitles_type: str = "auto_generated"


def parse_user_inputs(user_inputs: Dict[str, Any]) -> YouTubeSubtitleConfig:
    """
    解析并规范化 user_inputs -> YouTubeSubtitleConfig
    - 兼容 spider_info 为 list[dict] 或 dict
    - 统一键名、提供默认值，避免 KeyError
    """
    raw = user_inputs.get("spider_info", {})
    if isinstance(raw, list):
        if not raw:
            raise ValueError("spider_info 列表为空")
        raw = raw[0]
    if not isinstance(raw, dict):
        raise TypeError(f"spider_info 必须是 dict 或 list[dict]，当前为 {type(raw)}")

    info = _normalize_keys(raw)

    video_id = info.get("video_id")
    if not video_id:
        raise ValueError("video_id 为必填字段")

    domain = info.get("domain", "https://www.youtube.com")
    proxy_region = info.get("proxy_region")
    subtitles_language = _coerce_language(info.get("subtitles_language"))
    subtitles_type = _coerce_subtitles_type(info.get("subtitles_type"))

    cfg = YouTubeSubtitleConfig(
        video_id=video_id,
        domain=domain,
        proxy_region=proxy_region,
        subtitles_language=subtitles_language,
        subtitles_type=subtitles_type,
    )

    logger.info("已规范化配置: %s", cfg)
    return cfg


# =========================
# 下载实现（支持假数据与真实下载）
# =========================

def _sanitize_proxy(proxy: Optional[str]) -> Optional[str]:
    """
    规范化代理字符串，兼容以下常见输入：
    - http://user:pwd@host:port
    - socks5://user:pwd@host:port
    - user:pwd@host:port   -> 自动补全 http://
    - host:port            -> 自动补全 http://
    异常或缺失时返回 None（忽略代理）。
    """
    if not proxy:
        return None
    p = str(proxy).strip()
    if not p:
        return None

    # 已包含协议
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", p):
        return p

    # user:pwd@host:port
    if "@" in p and p.rsplit("@", 1)[-1].count(":") == 1:
        return "http://" + p

    # host:port
    if re.match(r"^[\w\.\-]+:\d{2,5}$", p):
        return "http://" + p

    logger.warning("代理字符串格式异常，已忽略：%s", p)
    return None


def _clear_old_caption_files(video_id: str) -> None:
    """根据 video_id 清理旧的字幕文件，避免多次运行时干扰。"""
    patterns = [
        os.path.join(OUTPUT_DIR, f"{video_id}.*.vtt"),
        os.path.join(OUTPUT_DIR, f"{video_id}.*.srt"),
        os.path.join(OUTPUT_DIR, f"{video_id}.vtt"),
        os.path.join(OUTPUT_DIR, f"{video_id}.srt"),
    ]
    removed = 0
    for p in patterns:
        for f in glob.glob(p):
            try:
                os.remove(f)
                removed += 1
            except Exception as e:
                logger.warning("删除旧文件失败 %s: %s", f, e)
    if removed:
        logger.info("已清理旧字幕文件 %d 个（video_id=%s）", removed, video_id)


def download_youtube_subtitles(cfg: YouTubeSubtitleConfig, proxy: Optional[str]) -> Dict[str, Any]:
    """
    下载字幕，返回统一结果：
    {
        "captions": <str>,                 # 字幕全文（可能为空串）
        "bytes": <int>,                    # 字节数
        "subtitle_type_resolved": <str>,   # 'auto_generated'/'manual'/'all' 中实际命中者
        "language_resolved": <str>,        # 实际匹配到的语言（可能与请求不同，如回退）
        "video_link_count": <int>,         # 下载命中的字幕文件数量
    }
    """
    # —— 假数据模式：不访问网络，便于联调 ——
    if ENABLE_FAKE_MODE:
        fake_text = f"# captions for {cfg.video_id} ({cfg.subtitles_language}/{cfg.subtitles_type})"
        return {
            "captions": fake_text,
            "bytes": len(fake_text.encode("utf-8")),
            "subtitle_type_resolved": cfg.subtitles_type,
            "language_resolved": cfg.subtitles_language,
            "video_link_count": 1,
        }

    try:
        import yt_dlp
    except ImportError:
        raise RuntimeError("缺少依赖：未安装 yt-dlp，请先执行 `pip install yt-dlp`")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 清理旧文件（避免前一次运行的文件干扰）
    if CLEAR_OLD_FILES:
        _clear_old_caption_files(cfg.video_id)

    # 代理
    proxy_url = _sanitize_proxy(proxy)

    # 语言优先：exact -> 前缀（zh-CN -> zh）
    desired = (cfg.subtitles_language or "zh").lower()
    base_prefix = desired.split("-")[0]
    subs_langs: List[str] = []
    for item in (desired, base_prefix):
        if item and item not in subs_langs:
            subs_langs.append(item)

    # 类型选择
    want_auto = cfg.subtitles_type in ("auto_generated", "all")
    want_manual = cfg.subtitles_type in ("manual", "all")

    ydl_opts = {
        "skip_download": True,            # 不下视频主体
        "writesubtitles": want_manual,    # 手动字幕
        "writeautomaticsub": want_auto,   # 自动字幕
        "subtitleslangs": subs_langs,     # 语言
        "subtitlesformat": "vtt",         # 统一 vtt
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "outtmpl": os.path.join(OUTPUT_DIR, f"{cfg.video_id}.%(ext)s"),
    }
    if proxy_url:
        ydl_opts["proxy"] = proxy_url

    url = f"{cfg.domain.rstrip('/')}/watch?v={cfg.video_id}"

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

        # 可用轨道
        manual_tracks = info.get("subtitles") or {}
        auto_tracks = info.get("automatic_captions") or {}

        def _norm(code: str) -> str:
            return (code or "").lower().replace("_", "-")

        # 选择最合适语言：exact -> 前缀 -> 任意
        chosen_lang = None
        all_langs = set(list(manual_tracks.keys()) + list(auto_tracks.keys()))
        for l in all_langs:
            if _norm(l) == _norm(desired):
                chosen_lang = l
                break
        if not chosen_lang:
            for l in all_langs:
                if _norm(l).startswith(base_prefix):
                    chosen_lang = l
                    break
        if not chosen_lang and all_langs:
            chosen_lang = list(all_langs)[0]

        # 仅下载字幕
        ydl.download([url])

    # 查找下载后的文件
    patterns = [
        os.path.join(OUTPUT_DIR, f"{cfg.video_id}.*.vtt"),
        os.path.join(OUTPUT_DIR, f"{cfg.video_id}.*.srt"),
        os.path.join(OUTPUT_DIR, f"{cfg.video_id}.vtt"),
        os.path.join(OUTPUT_DIR, f"{cfg.video_id}.srt"),
    ]
    files: List[str] = []
    for p in patterns:
        files.extend(glob.glob(p))
    files = sorted(set(files))

    if not files:
        raise RuntimeError("未找到下载后的字幕文件（可能该视频无字幕或下载被拦截）")

    # 优先匹配语言标记的文件（文件名中包含 .<lang>.）
    best_file = None
    if chosen_lang:
        token_exact = f".{chosen_lang}."
        for f in files:
            if token_exact in f:
                best_file = f
                break
        if not best_file:
            token_prefix = f".{base_prefix}"
            for f in files:
                if token_prefix in os.path.basename(f):
                    best_file = f
                    break

    # 若仍未命中，取最近修改的一个
    if not best_file:
        best_file = max(files, key=os.path.getmtime)

    # 读取内容与大小
    text, size = "", 0
    if best_file and os.path.exists(best_file):
        size = os.path.getsize(best_file)
        try:
            with open(best_file, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
        except Exception as e:
            logger.warning("读取字幕文件失败：%s", e)

    # 粗略判定实际类型（根据轨道集合）
    resolved_type = cfg.subtitles_type
    lang_for_judge = chosen_lang or desired
    if want_manual and lang_for_judge in (manual_tracks or {}):
        resolved_type = "manual"
    elif want_auto and lang_for_judge in (auto_tracks or {}):
        resolved_type = "auto_generated"

    return {
        "captions": text or "",
        "bytes": int(size),
        "subtitle_type_resolved": resolved_type,
        "language_resolved": chosen_lang or cfg.subtitles_language,
        "video_link_count": len(files),
    }


# =========================
# 任务包装与统计（含重试）
# =========================

def taipei_now() -> datetime:
    """获取台北时区当前时间（脚本运行环境若无 tz，可按 +08:00 近似）"""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo("Asia/Taipei")
        return datetime.now(tz)
    except Exception:
        return datetime.now(timezone(timedelta(hours=8)))


def make_task_id(now: Optional[datetime] = None) -> str:
    now = now or taipei_now()
    return f"task_{now.strftime('%Y%m%d%H%M%S')}_{os.urandom(8).hex()[:8]}"


def make_cos_key(task_id: str, now: Optional[datetime] = None) -> str:
    now = now or taipei_now()
    return f"{COS_PREFIX}/{now.year:04d}/{now.month:02d}/{now.day:02d}/{task_id}"


def ensure_csv_header(path: str) -> None:
    path_obj = Path(path)
    if not path_obj.exists():
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        with path_obj.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "task_id",
                "video_id",
                "subtitles_language",
                "subtitles_type",
                "result_total",
                "len_result",
                "total_time",
                "size_in_bytes",
                "error_rate",
                "error_number",
                "error",
                "cos_key",
                "video_link_count",
            ])


def append_csv_row(path: str, row: Dict[str, Any]) -> None:
    ensure_csv_header(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row.get("task_id"),
            row.get("video_id"),
            row.get("subtitles_language"),
            row.get("subtitles_type"),
            row.get("result_total"),
            row.get("len_result"),
            row.get("total_time"),
            row.get("size_in_bytes"),
            row.get("error_rate"),
            row.get("error_number"),
            row.get("error"),
            row.get("cos_key"),
            row.get("video_link_count"),
        ])


def run_once(user_inputs: Dict[str, Any]) -> Dict[str, Any]:
    """
    跑一次任务（含重试），返回统计字典并写入 CSV。
    """
    start = time.perf_counter()
    now = taipei_now()

    task_id = make_task_id(now)
    cos_key = make_cos_key(task_id, now)

    proxy = user_inputs.get("proxy")
    spider_errors = bool(user_inputs.get("spider_errors", True))

    stats = {
        "task_id": task_id,
        "video_id": "N/A",
        "subtitles_language": "N/A",
        "subtitles_type": "N/A",
        "result_total": 0,
        "len_result": 0,
        "total_time": 0,
        "size_in_bytes": 0,
        "error_rate": 1.0,
        "error_number": 1,
        "error": "",
        "cos_key": cos_key,
        "video_link_count": 0,
    }

    try:
        cfg = parse_user_inputs(user_inputs)
        last_err: Optional[Exception] = None

        for attempt in range(MAX_RETRIES + 1):
            try:
                result = download_youtube_subtitles(cfg, proxy)
                # 成功统计
                captions = result.get("captions", "")
                size_in_bytes = int(result.get("bytes") or len(captions.encode("utf-8")))
                lang_resolved = result.get("language_resolved", cfg.subtitles_language)
                type_resolved = result.get("subtitle_type_resolved", cfg.subtitles_type)
                link_count = int(result.get("video_link_count") or 0)
                total_time = round(time.perf_counter() - start, 3)

                stats.update({
                    "video_id": cfg.video_id,
                    "subtitles_language": lang_resolved,
                    "subtitles_type": type_resolved,
                    "result_total": 1 if captions else 0,
                    "len_result": len(captions),
                    "total_time": total_time,
                    "size_in_bytes": size_in_bytes,
                    "error_rate": 0.0,
                    "error_number": 0,
                    "error": "",
                    "cos_key": cos_key,
                    "video_link_count": link_count,
                })
                logger.info("任务成功[%s]（尝试 %d/%d）", cfg.video_id, attempt + 1, MAX_RETRIES + 1)
                break
            except Exception as e:
                last_err = e
                logger.warning("下载失败（尝试 %d/%d）: %s", attempt + 1, MAX_RETRIES + 1, e)
                time.sleep(0.5)  # 小停顿后再试

        if stats["error_rate"] == 1.0 and last_err:
            # 全部尝试都失败
            total_time = round(time.perf_counter() - start, 3)
            stats["total_time"] = total_time
            stats["error"] = f"runner-exception: {last_err.__class__.__name__}: {last_err}"
            logger.error("任务失败: %s", stats["error"], exc_info=spider_errors)

    except Exception as e:
        total_time = round(time.perf_counter() - start, 3)
        stats["total_time"] = total_time
        err_msg = f"runner-exception: {e.__class__.__name__}: {e}"
        stats["error"] = err_msg
        logger.error("任务失败: %s", err_msg, exc_info=spider_errors)

    try:
        append_csv_row(CSV_PATH, stats)
        logger.info("CSV 已保存: %s", CSV_PATH)
    except Exception as e:
        logger.error("写入 CSV 失败: %s", e)

    _print_human_readable(stats)
    return stats


def run_multiple(user_inputs: Dict[str, Any], runs: int, delay_seconds: float) -> List[Dict[str, Any]]:
    """
    连续运行多次，返回每次的统计列表。
    """
    results: List[Dict[str, Any]] = []
    for i in range(runs):
        logger.info("==== Run %d/%d ====", i + 1, runs)
        stats = run_once(user_inputs)
        results.append(stats)
        if i < runs - 1 and delay_seconds > 0:
            time.sleep(delay_seconds)
    return results


def _print_human_readable(stats: Dict[str, Any]) -> None:
    lines = [
        "Result statistics:",
        f"  task_id           : {stats.get('task_id')}",
        f"  video_id          : {stats.get('video_id')}",
        f"  subtitles_language: {stats.get('subtitles_language')}",
        f"  subtitles_type    : {stats.get('subtitles_type')}",
        f"  result_total      : {stats.get('result_total')}",
        f"  len_result        : {stats.get('len_result')}",
        f"  total_time        : {stats.get('total_time')}",
        f"  size_in_bytes     : {stats.get('size_in_bytes')}",
        f"  error_rate        : {stats.get('error_rate')}",
        f"  error_number      : {stats.get('error_number')}",
        f"  error             : {stats.get('error') or '(none)'}",
        f"  cos_key           : {stats.get('cos_key')}",
        f"  video_link_count  : {stats.get('video_link_count')}",
        "",
        f"All done. CSV saved to: {CSV_PATH}",
    ]
    print("\n".join(lines))


# =========================
# 命令行调试入口
# =========================

if __name__ == "__main__":
    # 读取命令行 JSON，或使用默认样例
    if len(sys.argv) > 1:
        try:
            ui = json.loads(sys.argv[1])
        except Exception as e:
            print(f"解析命令行 JSON 失败: {e}")
            sys.exit(1)
    else:
        # 默认样例（与日志一致）
        ui = {
            "spider_id": "youtube_subtitle_download",
            "spider_errors": True,
            "proxy": "td-customer-GH43726:GH43726@:9999",
            # 允许覆盖运行次数：也可以在这里加 "runs": 5
            "spider_info": [{
                "video_id": "LHCob76kigA",
                "domain": "https://www.youtube.com",
                "proxy_region": "us",
                "subtitles_language": "zh",
                "subtitles_type": "auto_generated",
            }],
        }

    runs = int(ui.get("runs") or RUN_TIMES)
    run_multiple(ui, runs, RUN_DELAY_SECONDS)
