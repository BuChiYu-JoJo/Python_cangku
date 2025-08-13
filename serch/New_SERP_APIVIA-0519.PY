import aiohttp
import asyncio
import csv
import time
import os
from serpapi import GoogleSearch

# ====================
# 核心配置
# ====================
API_KEY = "40189d646304866b21035fa4d31b38ceae28d9fe6065d3b20b643831134a5aad"

ENABLED_ENGINES = {
    "duckduckgo": False,
    "yandex": False,
    "luna_yandex_proxy": True,
    "duckduckgo_proxy": True
}

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
            "引擎": "duckduckgo",
            "关键词": term,
            "请求编号": req_num,
            "是否成功": False,
            "状态码": None,
            "耗时(ms)": 0,
            "内容大小(KB)": 0,
            "错误信息": "",
            "时间戳": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        url = "https://serpapi.com/search.json"
        params = {
            "engine": "duckduckgo",
            "q": term,
            "kl": "us-en",
            "no_cache": "true",
            "api_key": API_KEY
        }

        start = time.time()
        try:
            async with session.get(url, params=params, timeout=TIMEOUT_SECONDS) as response:
                text = await response.text()
                elapsed = (time.time() - start) * 1000
                result["耗时(ms)"] = round(elapsed, 2)
                result["状态码"] = response.status
                result["内容大小(KB)"] = round(len(text.encode("utf-8")) / 1024, 2)

                if response.status != 200:
                    result["错误信息"] = f"HTTP错误: {response.status}"
                    return result

                if result["内容大小(KB)"] < MIN_CONTENT_SIZE_KB:
                    result["错误信息"] = f"内容过小"
                    return result

                with open(f"{OUTPUT_DIR}/duck_{term}_{req_num:03d}.json", "w", encoding="utf-8") as f:
                    f.write(text)

                result["是否成功"] = True
        except Exception as e:
            result["错误信息"] = str(e)
            result["耗时(ms)"] = round((time.time() - start) * 1000, 2)

        return result

# ====================
# Yandex SDK 请求
# ====================
def fetch_yandex_sdk(term, req_num):
    result = {
        "引擎": "yandex",
        "关键词": term,
        "请求编号": req_num,
        "是否成功": False,
        "状态码": 200,
        "耗时(ms)": 0,
        "内容大小(KB)": 0,
        "错误信息": "",
        "时间戳": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    params = {
        "api_key": API_KEY,
        "engine": "yandex",
        "text": term,
        "no_cache": "true"
    }

    start = time.time()
    try:
        search = GoogleSearch(params)
        data = search.get_dict()
        elapsed = (time.time() - start) * 1000
        result["耗时(ms)"] = round(elapsed, 2)

        content = str(data)
        size_kb = round(len(content.encode("utf-8")) / 1024, 2)
        result["内容大小(KB)"] = size_kb

        if not data or "error" in data:
            result["错误信息"] = data.get("error", "空响应")
            return result

        with open(f"{OUTPUT_DIR}/yandex_{term}_{req_num:03d}.json", "w", encoding="utf-8") as f:
            f.write(content)

        result["是否成功"] = True
    except Exception as e:
        result["错误信息"] = f"SDK异常: {str(e)}"
        result["耗时(ms)"] = round((time.time() - start) * 1000, 2)

    return result

# ====================
# Luna Proxy 请求（POST）
# ====================
async def fetch_luna_proxy(session, semaphore, term, req_num):
    async with semaphore:
        result = {
            "引擎": "yandex_proxy",
            "关键词": term,
            "请求编号": req_num,
            "是否成功": False,
            "状态码": None,
            "耗时(ms)": 0,
            "内容大小(KB)": 0,
            "错误信息": "",
            "时间戳": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        url = "http://170.106.180.18:9004/spiders"
        json_data = {
            "product_name": "luna",
            "search_url": f"https://yandex.com/search/?text={term.replace(' ', '+')}",
            "user_id": "1000179",
            "user_name": "s3412612",
            "tdynamics": "Fasle",
            "json": "1"
        }

        start = time.time()
        try:
            async with session.post(url, json=json_data, timeout=TIMEOUT_SECONDS) as response:
                text = await response.text()
                elapsed = (time.time() - start) * 1000
                result["耗时(ms)"] = round(elapsed, 2)
                result["状态码"] = response.status
                result["内容大小(KB)"] = round(len(text.encode("utf-8")) / 1024, 2)

                if response.status != 200:
                    result["错误信息"] = f"HTTP错误: {response.status}"
                    return result

                with open(f"{OUTPUT_DIR}/luna_{term}_{req_num:03d}.json", "w", encoding="utf-8") as f:
                    f.write(text)

                result["是否成功"] = True
        except Exception as e:
            result["错误信息"] = str(e)
            result["耗时(ms)"] = round((time.time() - start) * 1000, 2)

        return result


# ====================
# DuckDuckGo via Luna Proxy（POST）
# ====================
async def fetch_duckduckgo_luna_proxy(session, semaphore, term, req_num):
    async with semaphore:
        result = {
            "引擎": "duckduckgo_proxy",
            "关键词": term,
            "请求编号": req_num,
            "是否成功": False,
            "状态码": None,
            "耗时(ms)": 0,
            "内容大小(KB)": 0,
            "错误信息": "",
            "时间戳": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        url = "http://170.106.180.18:9004/spiders"
        json_data = {
            "product_name": "luna",
            "search_url": f"https://duckduckgo.com/?q={term.replace(' ', '+')}&rpl=1&ia=web",
            "user_id": "1000179",
            "user_name": "s3412612",
            "tdynamics": "Fasle",
            "json": "1"
        }

        start = time.time()
        try:
            async with session.post(url, json=json_data, timeout=TIMEOUT_SECONDS) as response:
                text = await response.text()
                elapsed = (time.time() - start) * 1000
                result["耗时(ms)"] = round(elapsed, 2)
                result["状态码"] = response.status
                result["内容大小(KB)"] = round(len(text.encode("utf-8")) / 1024, 2)

                if response.status != 200:
                    result["错误信息"] = f"HTTP错误: {response.status}"
                    return result

                with open(f"{OUTPUT_DIR}/duckproxy_{term}_{req_num:03d}.json", "w", encoding="utf-8") as f:
                    f.write(text)

                result["是否成功"] = True
        except Exception as e:
            result["错误信息"] = str(e)
            result["耗时(ms)"] = round((time.time() - start) * 1000, 2)

        return result


# ====================
# 报告生成器
# ====================
def write_report(engine, results):
    filename = f"{REPORT_DIR}/{engine}_报告_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "时间戳", "引擎", "关键词", "请求编号",
            "状态码", "是否成功", "耗时(ms)", "内容大小(KB)", "错误信息"
        ])
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    print(f"✅ 已保存报告: {filename}")

# ====================
# 主程序入口
# ====================
async def main():
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(CONCURRENCY)

    if ENABLED_ENGINES.get("yandex"):
        print("▶ 开始 Yandex（SDK）请求")
        yandex_tasks = [
            loop.run_in_executor(None, fetch_yandex_sdk, term, i + 1)
            for term in SEARCH_TERMS
            for i in range(PER_QUERY_REQUESTS)
        ]
        yandex_results = await asyncio.gather(*yandex_tasks)
        write_report("yandex", yandex_results)

    async with aiohttp.ClientSession() as session:
        if ENABLED_ENGINES.get("duckduckgo"):
            print("▶ 开始 DuckDuckGo（异步）请求")
            duck_tasks = [
                fetch_duckduckgo(session, semaphore, term, i + 1)
                for term in SEARCH_TERMS
                for i in range(PER_QUERY_REQUESTS)
            ]
            duck_results = await asyncio.gather(*duck_tasks)
            write_report("duckduckgo", duck_results)

        if ENABLED_ENGINES.get("luna_yandex_proxy"):
            print("▶ 开始 Luna Proxy 请求")
            luna_tasks = [
                fetch_luna_proxy(session, semaphore, term, i + 1)
                for term in SEARCH_TERMS
                for i in range(PER_QUERY_REQUESTS)
            ]
            luna_results = await asyncio.gather(*luna_tasks)
            write_report("luna", luna_results)



if __name__ == "__main__":
    asyncio.run(main())
