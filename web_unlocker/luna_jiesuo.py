import aiohttp
import asyncio
import time
import csv
import os
import json
from datetime import datetime
from urllib.parse import urlparse

AUTH_TOKEN = "qlyg8vudqoxls791n92h5a7xfap209j0qiutha910i5hltm1ug71no7h4ltis4fv"
API_ENDPOINT = "https://unlocker-api.lunaproxy.com/request"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {AUTH_TOKEN}"
}
URLS = ["http://ipinfo.io/"]
REQUESTS_PER_URL = 100
CONCURRENT_REQUESTS = 10

OUTPUT_CSV = "results.csv"
ERROR_LOG = "error.log"
HTML_DIR = "output_html"
os.makedirs(HTML_DIR, exist_ok=True)

results = []

def log_error(url, index, exception):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] Error on request {index} to {url}: {type(exception).__name__} - {exception}\n")

def save_html(content, index, url):
    domain = urlparse(url).hostname.replace('.', '_')
    filename = f"{index:03d}_{domain}.html"
    path = os.path.join(HTML_DIR, filename)
    with open(path, "wb") as f:
        f.write(content)

async def fetch(semaphore, target_url, index):
    async with semaphore:
        start_time = time.time()
        connector = aiohttp.TCPConnector(limit=1, force_close=True, enable_cleanup_closed=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                payload = json.dumps({"url": target_url})
                async with session.post(API_ENDPOINT, headers=HEADERS, data=payload, ssl=False) as response:
                    content = await response.read()
                    elapsed = time.time() - start_time
                    size_kb = round(len(content) / 1024, 2)
                    unlocked = "是" if size_kb > 100 else "否"

                    results.append({
                        "url": target_url,
                        "响应时间(s)": round(elapsed, 3),
                        "文件大小(KB)": size_kb,
                        "解锁情况": unlocked
                    })
                    save_html(content, index, target_url)
                    print(f"Completed {index} - {target_url} ({elapsed:.2f}s, {size_kb} KB, 解锁: {unlocked})")
            except Exception as e:
                log_error(target_url, index, e)
                print(f"[Error] {target_url} ({index}): {e}")

async def run():
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    tasks = []
    index = 1
    for url in URLS:
        for _ in range(REQUESTS_PER_URL):
            tasks.append(fetch(semaphore, url, index))
            index += 1
    await asyncio.gather(*tasks)

def save_to_csv():
    with open(OUTPUT_CSV, mode="w", newline='', encoding="utf-8") as f:
        fieldnames = ["url", "响应时间(s)", "文件大小(KB)", "解锁情况"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved {len(results)} records to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(run())
    save_to_csv()
