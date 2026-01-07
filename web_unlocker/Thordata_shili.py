import os
import json
import requests
import time   # ✅ 新增
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 配置区 =================

DOWNLOAD_URL = (
#    "https://serp.acen.http.321174.com/download?api_key=68cbe484673ec79e01d7399de06d6475&plat=1&task_id=fd1e7961ffbf428ba1e5192b958d7b97&type=json"
    "https://serp.acen.http.321174.com/download?api_key=869c34078dd6f072cedfafe220723454&plat=1&task_id=47c6ac907d4f4b21bc2aef36a61c1e48&type=json"
)

TOTAL_TIMES = 5       # 下载次数
CONCURRENCY = 1        # 并发数
TIMEOUT = 30           # 请求超时（秒）
OUTPUT_DIR = "downloads"
SLEEP_SECONDS = 0      # ✅ 每次下载前等待 1 秒

# ==========================================

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_task_id(url: str) -> str:
    """从 URL 中提取 task_id"""
    query = urlparse(url).query
    params = parse_qs(query)
    return params.get("task_id", ["unknown"])[0]

TASK_ID = get_task_id(DOWNLOAD_URL)

def download_once(index: int):
    """单次下载"""
    try:
        print(f"⏳ 等待 {SLEEP_SECONDS}s 后开始下载 #{index}")
        time.sleep(SLEEP_SECONDS)   # ✅ 核心：下载前延时

        print(f"⬇️  开始下载 #{index}")
        resp = requests.get(DOWNLOAD_URL, timeout=TIMEOUT)
        resp.raise_for_status()

        filename = f"{TASK_ID}_{index}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(resp.json(), f, ensure_ascii=False, indent=2)

        print(f"✅ 下载完成 #{index} → {filename}")
    except Exception as e:
        print(f"❌ 下载失败 #{index}: {e}")

def main():
    print(
        f"🚀 开始下载：task_id={TASK_ID}，"
        f"总次数={TOTAL_TIMES}，并发={CONCURRENCY}"
    )

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [
            executor.submit(download_once, i + 1)
            for i in range(TOTAL_TIMES)
        ]

        for _ in as_completed(futures):
            pass

    print("🎉 所有下载任务完成")

if __name__ == "__main__":
    main()
