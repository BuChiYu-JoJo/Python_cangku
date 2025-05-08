import aiohttp
import asyncio
import csv
import time
import os
from urllib.parse import quote
from aiohttp import BasicAuth

# ====================
# 核心配置区
# ====================

SEARCH_TERMS = [
    "Apple", "Bread", "Cheese", "Salmon", "Chocolate",
    "Spinach", "Yogurt", "Pasta", "Almond", "Eggplant"
]

# 启用的服务开关（True 表示启用）
SERVICES_ENABLED = {
    'thordata': False,
    'brightdata': False,
    'oxylabs': False,
    'serpapi': False,
    'yibiaopan': False,
    'jingpin': True
}

# 每个服务的请求结构配置
SERVICE_CONFIG = {
    'thordata': {
        'url': "http://170.106.180.18:9004/spiders",
        'method': 'POST',
        'headers': {"Content-Type": "application/json"},
        'param': lambda q: {
            "product_name": "luna",
            "search_url": f"https://www.google.com/search?q={quote(q)}",
            "user_id": "1000179",
            "user_name": "s3412612",
            "tdynamics": "False"
        }
    },
    'brightdata': {
        'url': "https://api.brightdata.com/request",
        'method': 'POST',
        'headers': {"Authorization": "Bearer ..."},
        'json_payload': lambda q: {
            "zone": "serp_api1",
            "url": f"https://www.google.com/search?q={q}",
            "format": "json"
        }
    },
    'oxylabs': {
        'url': "https://realtime.oxylabs.io/v1/queries",
        'method': 'POST',
        'auth': BasicAuth("YOUR_OXYLABS_USER", "YOUR_OXYLABS_PASS"),
        'json_payload': lambda q: {
            "source": "google_search",
            "query": q
        }
    },
    'serpapi': {
        'url': "https://serpapi.abcproxy.com/search",
        'method': 'GET',
        'params': lambda q: {
            "engine": "google",
            "q": q,
            "api_key": "YOUR_SERPAPI_KEY"
        }
    },
    'jingpin': {
        'url': "https://serpapi.com/search.json",
        'method': 'GET',
        'params': lambda q: {
            "engine": "google",
            "q": q,
            "api_key": "40189d646304866b21035fa4d31b38ceae28d9fe6065d3b20b643831134a5aad",
            "location": "Austin, Texas, United States",
            "google_domain": "google.com",
            "gl": "us",
            "hl": "en",
            "device": "desktop"
        }
    },
    'yibiaopan': {
        'url': "https://scraperapi.thordata.com/request",
        'method': 'POST',
        'headers': {
            "Content-Type": "application/json",
            "Authorization": "Bearer ..."
        },
        'json_payload': lambda q: {
            "url": f"https://www.google.com/search?q={q}"
        }
    }
}

# 每个关键词请求次数
PER_QUERY_REQUESTS = 100

# 每个请求超时时间（秒）
TIMEOUT_SECONDS = 20

# 最小有效响应体积（KB），小于此视为失败
MIN_CONTENT_SIZE_KB = 10

# 并发请求数上限
CONCURRENCY = 20

# 是否保存响应为空的请求结果（True 表示保存空内容）
SAVE_EMPTY_RESPONSE = False

# 是否保存所有响应文件（包括正常内容和小文件）
SAVE_RESPONSE_FILES = True

# ====================
# 异步请求处理器
# ====================
async def handle_request(session, semaphore, service, term, req_num):
    async with semaphore:
        config = SERVICE_CONFIG[service]
        start_time = time.time()
        result = {
            'service': service,
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
            request_args = {
                "method": config['method'],
                "url": config['url'],
                "headers": config.get('headers', {}),
                "timeout": aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
            }

            if 'param' in config:
                request_args["json"] = config['param'](term)
            elif 'json_payload' in config:
                request_args["json"] = config['json_payload'](term)
            elif 'params' in config:
                request_args["params"] = config['params'](term)

            if 'auth' in config:
                request_args["auth"] = config['auth']

            async with session.request(**request_args) as response:
                content = await response.text()
                result['status_code'] = response.status
                elapsed = (time.time() - start_time) * 1000
                result['elapsed_ms'] = elapsed if elapsed >= 1 else 1
                content_size = len(content.encode('utf-8'))
                result['content_size_kb'] = content_size / 1024

                # 检查返回类型
                if "text/html" in response.headers.get("Content-Type", ""):
                    result['error'] = "返回了 HTML 页面，可能 URL 或参数错误"
                    return result

                if response.status != 200:
                    result['error'] = f"HTTP错误: {response.status}"
                    return result

                if not content:
                    result['error'] = "空响应内容"
                    return result

                if content_size < MIN_CONTENT_SIZE_KB * 1024:
                    result['error'] = f"内容过小 ({result['content_size_kb']:.2f}KB)"
                    return result

                # 保存内容（受 SAVE_RESPONSE_FILES 控制）
                if SAVE_RESPONSE_FILES and (content_size > 0 or SAVE_EMPTY_RESPONSE):
                    folder = f"data/{service}"
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
            elapsed = (time.time() - start_time) * 1000
            result['elapsed_ms'] = elapsed if elapsed >= 1 else 1
            if result['elapsed_ms'] < 2:
                print(f"⚠️ 触发异常且耗时近0ms -> service={service}, term={term}, 错误: {e}")
            with open('error_log.txt', 'a', encoding='utf-8') as log:
                log.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 错误服务: {service}, 关键词: {term}, 错误: {e}\n")
            return result

# ====================
# 报告生成器
# ====================
def generate_report(service, stats):
    os.makedirs("reports", exist_ok=True)
    filename = f"reports/{service}_report_{time.strftime('%Y%m%d%H%M%S')}.csv"

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
                'service': service,
                '检索词': item['term'],
                '请求号': item['req_num'],
                '状态码': item['status_code'] or 'N/A',
                '是否成功': "成功" if item['success'] else "失败",
                '耗时(ms)': f"{item['elapsed_ms']:.2f}",
                '内容大小(KB)': f"{item['content_size_kb']:.2f}",
                '错误信息': item['error'][:100]
            })

    print(f"\n✅ 报告已生成: {filename}")
    return filename

# ====================
# 异步主程序
# ====================
async def main():
    print("=== 异步多服务验证工具 ===")
    print(f"并发数: {CONCURRENCY} | 超时设置: {TIMEOUT_SECONDS}s")

    async with aiohttp.ClientSession() as session:
        for service in SERVICES_ENABLED:
            if not SERVICES_ENABLED[service]:
                print(f"跳过禁用服务: {service}")
                continue

            print(f"\n▶ 开始处理服务: {service.upper()}")
            stats = []
            semaphore = asyncio.Semaphore(CONCURRENCY)

            tasks = []
            for term in SEARCH_TERMS:
                for req_num in range(1, PER_QUERY_REQUESTS + 1):
                    tasks.append(
                        handle_request(session, semaphore, service, term, req_num)
                    )

            total_tasks = len(tasks)
            for i in range(0, total_tasks, CONCURRENCY * 2):
                batch = tasks[i:i + CONCURRENCY * 2]
                results = await asyncio.gather(*batch)
                stats.extend(results)

                success_count = sum(1 for r in results if r['success'])
                print(f"▏已处理 {i+len(results)}/{total_tasks} | 本批成功率: {success_count/len(results):.1%}")

            generate_report(service, stats)

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
