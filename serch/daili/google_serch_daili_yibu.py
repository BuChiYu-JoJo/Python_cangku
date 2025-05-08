import aiohttp
import asyncio
import csv
import time
import os
from urllib.parse import quote

# ====================
# 配置区
# ====================

SEARCH_TERMS = [
    "Apple", "Bread", "Cheese", "Salmon", "Chocolate",
    "Spinach", "Yogurt", "Pasta", "Almond", "Eggplant"
]

PER_QUERY_REQUESTS = 5  # 可调小测试
TIMEOUT_SECONDS = 20
MIN_CONTENT_SIZE_KB = 10
CONCURRENCY = 5
SAVE_EMPTY_RESPONSE = False
SAVE_RESPONSE_FILES = True

HTTP_PROXY = "http://td-customer-serp_HhFgxd5euqcb2:JJtvFttulx7n@scraping.thordata.com:30001"
HTTPS_PROXY = "http://td-customer-serp_HhFgxd5euqcb2:JJtvFttulx7n@scraping.thordata.com:30002"

# ====================
# 异步请求处理器
# ====================
async def handle_request(session, semaphore, term, req_num):
    async with semaphore:
        start_time = time.time()
        result = {
            'service': 'thordata-proxy',
            'term': term,
            'req_num': req_num,
            'success': False,
            'status_code': None,
            'error': '',
            'content_size_kb': 0,
            'elapsed_ms': 0,
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            url = f"https://www.google.com/search?q={quote(term)}&hl=en&gl=us"
            proxy = HTTPS_PROXY if url.startswith("https://") else HTTP_PROXY

            async with session.get(url, proxy=proxy, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as response:
                content = await response.text()
                result['status_code'] = response.status
                elapsed = (time.time() - start_time) * 1000
                result['elapsed_ms'] = elapsed if elapsed >= 1 else 1
                content_size = len(content.encode('utf-8'))
                result['content_size_kb'] = content_size / 1024

                if response.status != 200:
                    result['error'] = f"HTTP错误: {response.status}"
                    return result

                if not content:
                    result['error'] = "空响应内容"
                    return result

                if content_size < MIN_CONTENT_SIZE_KB * 1024:
                    result['error'] = f"内容过小 ({result['content_size_kb']:.2f}KB)"
                    return result

                if SAVE_RESPONSE_FILES and (content_size > 0 or SAVE_EMPTY_RESPONSE):
                    folder = "data/thordata-proxy"
                    os.makedirs(folder, exist_ok=True)
                    filename = f"{term[:20].replace('/', '_')}_req{req_num:03d}.txt"
                    with open(os.path.join(folder, filename), 'w', encoding='utf-8') as f:
                        f.write(content)

                result['success'] = True
                return result

        except asyncio.TimeoutError:
            result['error'] = f"请求超时 ({TIMEOUT_SECONDS}s)"
            result['elapsed_ms'] = TIMEOUT_SECONDS * 1000
            return result
        except Exception as e:
            result['error'] = f"请求异常: {str(e)}"
            result['elapsed_ms'] = (time.time() - start_time) * 1000
            with open('error_log.txt', 'a', encoding='utf-8') as log:
                log.write(f"[{result['timestamp']}] term: {term}, 错误: {e}\n")
            return result

# ====================
# 报告生成器
# ====================
def generate_report(stats):
    os.makedirs("reports", exist_ok=True)
    filename = f"reports/thordata_proxy_report_{time.strftime('%Y%m%d%H%M%S')}.csv"

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'timestamp', 'service', '检索词', '请求号',
            '状态码', '是否成功', '耗时(ms)', '内容大小(KB)', '错误信息'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for item in stats:
            writer.writerow({
                'timestamp': item['timestamp'],
                'service': item['service'],
                '检索词': item['term'],
                '请求号': item['req_num'],
                '状态码': item['status_code'] or 'N/A',
                '是否成功': "成功" if item['success'] else "失败",
                '耗时(ms)': f"{item['elapsed_ms']:.2f}",
                '内容大小(KB)': f"{item['content_size_kb']:.2f}",
                '错误信息': item['error'][:100]
            })

    print(f"\n✅ 报告已生成: {filename}")

# ====================
# 主程序入口
# ====================
async def main():
    print("=== Thordata 代理异步请求工具 ===")
    print(f"并发数: {CONCURRENCY} | 超时: {TIMEOUT_SECONDS}s")

    async with aiohttp.ClientSession() as session:
        stats = []
        semaphore = asyncio.Semaphore(CONCURRENCY)
        tasks = []

        for term in SEARCH_TERMS:
            for req_num in range(1, PER_QUERY_REQUESTS + 1):
                tasks.append(handle_request(session, semaphore, term, req_num))

        total_tasks = len(tasks)
        for i in range(0, total_tasks, CONCURRENCY * 2):
            batch = tasks[i:i + CONCURRENCY * 2]
            results = await asyncio.gather(*batch)
            stats.extend(results)

            success_count = sum(1 for r in results if r['success'])
            print(f"▏已处理 {i+len(results)}/{total_tasks} | 本批成功率: {success_count/len(results):.1%}")

        generate_report(stats)

if __name__ == "__main__":
    asyncio.run(main())
