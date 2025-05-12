import aiohttp
import asyncio
import time
import csv
import os
from datetime import datetime
from urllib.parse import urlparse
from asyncio import Semaphore

# === 配置参数 ===
PROXY = "http://dnfccv-zone-unblock-region-us:Zz123456789@@unblock.novada.pro:7799"
URLS = ["http://ipinfo.io/"]
REQUESTS_PER_URL = 100
CONCURRENT_REQUESTS = 1  # 并发请求
OUTPUT_CSV = "results.csv"
ERROR_LOG = "error.log"
HTML_DIR = "output_html"

# === 可选请求头设置 ===
HEADERS = {
    "X-Novada-Render-Type": "html",
    "X-Render-Type": "html",
}

results = []

# 创建保存 HTML 的目录
os.makedirs(HTML_DIR, exist_ok=True)

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

async def fetch(url, index, semaphore: Semaphore):
    async with semaphore:
        start_time = time.time()
        connector = aiohttp.TCPConnector(limit=1, force_close=True, enable_cleanup_closed=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            try:
                async with session.get(url, proxy=PROXY, headers=HEADERS, ssl=False) as response:
                    content = await response.read()
                    elapsed = time.time() - start_time
                    size_kb = round(len(content) / 1024, 2)
                    unlocked = "是" if size_kb > 100 else "否"

                    results.append({
                        "url": url,
                        "响应时间(s)": round(elapsed, 3),
                        "文件大小(KB)": size_kb,
                        "解锁情况": unlocked
                    })
                    save_html(content, index, url)
                    print(f"Completed {index} - {url} ({elapsed:.2f}s, {size_kb} KB, 解锁: {unlocked})")
            except Exception as e:
                log_error(url, index, e)
                print(f"[Error] {url} ({index}): {e}")

async def run():
    semaphore = Semaphore(CONCURRENT_REQUESTS)
    tasks = []
    index = 1
    for url in URLS:
        for _ in range(REQUESTS_PER_URL):
            tasks.append(fetch(url, index, semaphore))
            index += 1
    await asyncio.gather(*tasks)

def save_to_csv():
    with open(OUTPUT_CSV, mode="w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "响应时间(s)", "文件大小(KB)", "解锁情况"])
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved {len(results)} records to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(run())
    save_to_csv()
