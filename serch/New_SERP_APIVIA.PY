import aiohttp
import asyncio
import csv
import time
import os
from serpapi import GoogleSearch

# ====================
# 核心配置区
# ====================
API_KEY = "40189d646304866b21035fa4d31b38ceae28d9fe6065d3b20b643831134a5aad"
SEARCH_TERMS = [    "Apple", "Bread", "Cheese", "Salmon", "Chocolate",
    "Spinach", "Yogurt", "Pasta", "Almond", "Eggplant"]
PER_QUERY_REQUESTS = 100
CONCURRENCY = 1
TIMEOUT_SECONDS = 60
MIN_CONTENT_SIZE_KB = 1

OUTPUT_DIR = "data"
REPORT_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# ====================
# DuckDuckGo 异步请求
# ====================
async def fetch_duckduckgo(session, semaphore, term, req_num):
    async with semaphore:
        result = {
            "engine": "duckduckgo",
            "term": term,
            "req_num": req_num,
            "success": False,
            "status_code": None,
            "elapsed_ms": 0,
            "content_size_kb": 0,
            "error": "",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        url = "https://serpapi.com/search.json"
        params = {
            "engine": "duckduckgo",
            "q": term,
            "kl": "us-en",
            "no_cache": "true",
            "api_key": API_KEY
        }

        start_time = time.time()
        try:
            async with session.get(url, params=params, timeout=TIMEOUT_SECONDS) as response:
                text = await response.text()
                elapsed = (time.time() - start_time) * 1000
                result["elapsed_ms"] = round(elapsed, 2)
                result["status_code"] = response.status
                result["content_size_kb"] = round(len(text.encode("utf-8")) / 1024, 2)

                if response.status != 200:
                    result["error"] = f"HTTP错误: {response.status}"
                    return result

                if result["content_size_kb"] < MIN_CONTENT_SIZE_KB:
                    result["error"] = f"内容过小: {result['content_size_kb']}KB"
                    return result

                # 保存内容
                filename = f"{OUTPUT_DIR}/duckduckgo_{term}_{req_num:03d}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(text)

                result["success"] = True
        except Exception as e:
            result["error"] = f"异常: {str(e)}"
            result["elapsed_ms"] = round((time.time() - start_time) * 1000, 2)

        return result

# ====================
# Yandex 同步请求（SDK）
# ====================
def fetch_yandex_sdk(term, req_num):
    result = {
        "engine": "yandex",
        "term": term,
        "req_num": req_num,
        "success": False,
        "status_code": 200,
        "elapsed_ms": 0,
        "content_size_kb": 0,
        "error": "",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    params = {
        "api_key": API_KEY,
        "engine": "yandex",
        "text": term,
        "no_cache": "true"
    }

    start_time = time.time()
    try:
        search = GoogleSearch(params)
        data = search.get_dict()
        elapsed = (time.time() - start_time) * 1000
        result["elapsed_ms"] = round(elapsed, 2)

        content = str(data)
        size_kb = round(len(content.encode("utf-8")) / 1024, 2)
        result["content_size_kb"] = size_kb

        if not data or "error" in data:
            result["error"] = data.get("error", "返回空或无效内容")
            return result

        # 保存
        filename = f"{OUTPUT_DIR}/yandex_{term}_{req_num:03d}.json"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)

        result["success"] = True
    except Exception as e:
        result["error"] = f"SDK异常: {str(e)}"
        result["elapsed_ms"] = round((time.time() - start_time) * 1000, 2)

    return result

# ====================
# 报告生成器
# ====================
def write_report(engine, results):
    filename = f"{REPORT_DIR}/{engine}_report_{time.strftime('%Y%m%d%H%M%S')}.csv"
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "engine", "term", "req_num",
            "status_code", "success", "elapsed_ms",
            "content_size_kb", "error"
        ])
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    print(f"✅ {engine} 报告生成：{filename}")

# ====================
# 主异步函数
# ====================
async def main():
    print("▶ 开始 Yandex SDK 请求")
    yandex_results = []
    loop = asyncio.get_event_loop()

    yandex_tasks = [
        loop.run_in_executor(None, fetch_yandex_sdk, term, i + 1)
        for term in SEARCH_TERMS
        for i in range(PER_QUERY_REQUESTS)
    ]
    yandex_results = await asyncio.gather(*yandex_tasks)

    write_report("yandex", yandex_results)


    print("▶ 开始 DuckDuckGo 异步请求")
    duck_results = []
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_duckduckgo(session, semaphore, term, i + 1)
            for term in SEARCH_TERMS
            for i in range(PER_QUERY_REQUESTS)
        ]
        duck_results = await asyncio.gather(*tasks)

    write_report("duckduckgo", duck_results)

if __name__ == "__main__":
    asyncio.run(main())
