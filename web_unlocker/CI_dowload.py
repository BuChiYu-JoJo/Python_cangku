import os
import json
import requests
import time
import sys
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

DOWNLOAD_URL = os.getenv("DOWNLOAD_URL")
TOTAL_TIMES = int(os.getenv("TOTAL_TIMES", 5))
CONCURRENCY = int(os.getenv("CONCURRENCY", 1))
TIMEOUT = 30
OUTPUT_DIR = "downloads"
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", 1))

if not DOWNLOAD_URL:
    print("❌ DOWNLOAD_URL 未设置")
    sys.exit(1)

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_task_id(url: str) -> str:
    query = urlparse(url).query
    params = parse_qs(query)
    return params.get("task_id", ["unknown"])[0]

TASK_ID = get_task_id(DOWNLOAD_URL)

def download_once(index: int) -> bool:
    try:
        time.sleep(SLEEP_SECONDS)
        resp = requests.get(DOWNLOAD_URL, timeout=TIMEOUT)
        resp.raise_for_status()

        filename = f"{TASK_ID}_{index}.json"
        with open(os.path.join(OUTPUT_DIR, filename), "w", encoding="utf-8") as f:
            json.dump(resp.json(), f, ensure_ascii=False, indent=2)

        print(f"✅ 下载成功 #{index}")
        return True
    except Exception as e:
        print(f"❌ 下载失败 #{index}: {e}")
        return False

def main():
    failed = 0

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [
            executor.submit(download_once, i + 1)
            for i in range(TOTAL_TIMES)
        ]

        for future in as_completed(futures):
            if not future.result():
                failed += 1

    if failed > 0:
        print(f"❌ 共 {failed} 个任务失败")
        sys.exit(1)

    print("🎉 所有下载成功")

if __name__ == "__main__":
    main()
