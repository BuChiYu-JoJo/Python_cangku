# -*- coding: utf-8 -*-
"""
YouTube 测试脚本（不上传，保留本地 temp_file）
- 使用低层接口 youtube_multiple_resp（不会触发上传与清理）
- 大量视频ID池 + 可额外自动生成随机ID
- 动态 task_id
- 可配置调用次数、重试次数、是否去重抽样
- 每次调用把必要信息追加写入 CSV
"""
import asyncio, datetime, os, sys, uuid, random, csv, string
from pathlib import Path

# === 项目根路径（按你的真实路径改） ===
PROJECT_ROOT = "/home/thor-scraper-backend"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from projects.scraper_01_youtube.rep_01_01_detail import youtube_multiple_resp

# ========== 配置 ==========
NUM_RUNS = 40                        # 本次测试调用次数（可调大）
PRODUCT_NAME = "thordata"
YOUTUBE_DOMAIN = "https://www.youtube.com"
PROXY_REGION = "us"
RETRY_TIMES = 5
CSV_OUTPUT = "./youtube_test_runs_local.csv"
SLEEP_BETWEEN_RUNS = 0               # 两次调用之间的等待秒数（0 表示不等待）
SAMPLE_WITHOUT_REPLACEMENT = True    # True：不放回抽样（需池子≥NUM_RUNS）；False：允许重复

# 自动额外生成的随机ID数量（0 表示不生成）
EXTRA_RANDOM_IDS = 60

# ========== 预置视频ID池（可继续扩充） ==========
VIDEO_ID_POOL_BASE = [
    "dQw4w9WgXcQ","kXYiU_JCYtU","3JZ_D3ELwOQ","9bZkp7q19f0","CevxZvSJLk8",
    "hTWKbfoikeg","fJ9rUzIMcZQ","e-ORhEE9VVg","JGwWNGJdvx8","OPf0YbXqDm0",
    "2Vv-BfVoq4g","60ItHLz5WEA","kJQP7kiw5Fk","09R8_2nJtjg","uelHwf8o7_U",
    "oRdxUFDoQe0","ktvTqknDobU","gCYcHz2k5x0","hT_nvWreIhg","SlPhMPnQ58k",
    "M7lc1UVf-VE","GxLD5wYwFJY","iS1g8G_njx8","pRpeEdMmmQ0","LsoLEjrDogU",
    "rub90zZ3e5c","aJOTlE1K90k","dk9uNWPP7EA","hLQl3WQQoQ0","vNoKguSdy4Y",
    "t4H_Zoh7G5A","uelHwf8o7_U","RgKAFK5djSk","nYh-n7EOtMA","YQHsXMglC9A",
    "iS1g8G_njx8","nfWlot6h_JM","kOkQ4T5WO9E","uelHwf8o7_U","fRh_vgS2dFE",
    "RgKAFK5djSk","hLQl3WQQoQ0","JGwWNGJdvx8","tAGnKpE4NCI","pXRviuL6vMY",
    "34Na4j8AVgA","mWRsgZuwf_8","LHCob76kigA","ZyhrYis509A","pBuZEGYXA6E",
    "2vjPBrBU-TM","k2qgadSvNyU","mYfJxlgR2jw","09R8_2nJtjg","l9PxOanFjxQ",
    "cg1MgGvGgvE","aWIE0PX1uXk","YVkUvmDQ3HY","kXYiU_JCYtU","MwpMEbgC7DA",
    "pRpeEdMmmQ0","tVj0ZTS4WF4","iS1g8G_njx8","hT_nvWreIhg","uelHwf8o7_U",
    "pRpeEdMmmQ0","KlyXNRrsk4A","Rub90zZ3e5c","tAGnKpE4NCI","M7lc1UVf-VE",
]

# ====== 工具函数 ======
def gen_task_id() -> str:
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    return f"task_{ts}_{uuid.uuid4().hex[:8]}"

def ensure_csv_header(csv_path: str, header: list) -> None:
    pf = Path(csv_path)
    if not pf.exists():
        pf.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)

def append_csv_row(csv_path: str, row: list) -> None:
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

def random_youtube_id(n: int) -> list:
    """生成 n 个随机形如 [A-Za-z0-9_-]{11} 的ID"""
    alphabet = string.ascii_letters + string.digits + "-_"
    return ["".join(random.choice(alphabet) for _ in range(11)) for __ in range(n)]

def build_id_pool() -> list:
    pool = list(dict.fromkeys(VIDEO_ID_POOL_BASE))  # 去重保持顺序
    if EXTRA_RANDOM_IDS > 0:
        pool.extend(random_youtube_id(EXTRA_RANDOM_IDS))
    return pool

def pick_ids_for_run(pool: list, num: int, without_replacement: bool) -> list:
    if without_replacement:
        if len(pool) < num:
            # 池子不够时，自动补随机ID
            pool = pool + random_youtube_id(num - len(pool) + 20)
        return random.sample(pool, num)
    # 允许重复
    return [random.choice(pool) for _ in range(num)]

# ====== 单次执行 ======
async def run_once(video_id: str) -> dict:
    task_id = gen_task_id()
    now = datetime.datetime.now()

    # 构造 cos_key（用于本地临时文件命名）
    cos_key = f"scrapers/{PRODUCT_NAME}/{now.strftime('%Y/%m/%d')}/{task_id}"

    user_inputs = {
        "spider_errors": True,
        "spider_id": "youtube_product_by-id",
        "spider_parameters": [
            {
                "video_id": video_id,
                "domain": YOUTUBE_DOMAIN,
                "proxy_region": PROXY_REGION,
            }
        ]
    }

    # 错误容器必须是 set（内部会用 .add）
    error = set()

    # 调用低层接口（不会触发上传与删除）
    result_num, total_time, size_in_bytes, error_number, error_set = \
        await youtube_multiple_resp(
            user_inputs=user_inputs,
            cos_key=cos_key,
            error=error,
            retry_times=RETRY_TIMES
        )

    record = {
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
        "task_id": task_id,
        "video_id": video_id,
        "domain": YOUTUBE_DOMAIN,
        "proxy_region": PROXY_REGION,
        "result_num": int(result_num),
        "total_time": round(float(total_time), 4),
        "size_in_bytes": int(size_in_bytes),
        "error_number": int(error_number),
        "error": "; ".join(sorted(map(str, error_set))) if error_set else "",
        "cos_key": cos_key,
    }
    return record

# ====== 主流程（多次执行） ======
async def main():
    # 确保 temp_file 目录存在（你的保存函数默认写到这个目录）
    os.makedirs("/home/thor-scraper-backend/temp_file", exist_ok=True)

    header = [
        "timestamp","task_id","video_id","domain","proxy_region",
        "result_num","total_time","size_in_bytes","error_number","error","cos_key",
    ]
    ensure_csv_header(CSV_OUTPUT, header)

    # 构建视频ID池 & 选择本次要跑的ID
    pool = build_id_pool()
    video_ids = pick_ids_for_run(pool, NUM_RUNS, SAMPLE_WITHOUT_REPLACEMENT)

    print(f"Starting YouTube test runs: NUM_RUNS={NUM_RUNS}, pool_size={len(pool)}, selected={len(video_ids)}")

    for i, vid in enumerate(video_ids, 1):
        print(f"\n=== Run {i}/{NUM_RUNS} — {vid} ===")
        try:
            r = await run_once(vid)
        except Exception as e:
            # 兜底：异常也落一条记录
            task_id = gen_task_id()
            r = {
                "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
                "task_id": task_id,
                "video_id": vid,
                "domain": YOUTUBE_DOMAIN,
                "proxy_region": PROXY_REGION,
                "result_num": 0,
                "total_time": 0,
                "size_in_bytes": 0,
                "error_number": 1,
                "error": f"runner-exception: {e}",
                "cos_key": f"scrapers/{PRODUCT_NAME}/{datetime.datetime.now().strftime('%Y/%m/%d')}/{task_id}",
            }

        # 控制台打印
        print("Result statistics:")
        for k in ["task_id","video_id","result_num","total_time","size_in_bytes","error_number","error","cos_key"]:
            print(f"  {k:12s}: {r[k]}")

        # 追加写入 CSV
        append_csv_row(CSV_OUTPUT, [
            r["timestamp"], r["task_id"], r["video_id"], r["domain"], r["proxy_region"],
            r["result_num"], r["total_time"], r["size_in_bytes"], r["error_number"], r["error"], r["cos_key"],
        ])

        if SLEEP_BETWEEN_RUNS > 0 and i < NUM_RUNS:
            await asyncio.sleep(SLEEP_BETWEEN_RUNS)

    print(f"\nAll done. CSV saved to: {os.path.abspath(CSV_OUTPUT)}")

if __name__ == "__main__":
    asyncio.run(main())
