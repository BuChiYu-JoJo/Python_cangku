import requests
import concurrent.futures
import csv
import time
import os
from urllib.parse import quote
from rich.progress import Progress

# 配置参数
QUERIES = [
    "pizza", "sushi", "burger", "pasta", "salad",
    "steak", "taco", "ramen", "curry", "sandwich"
]
REQUESTS_PER_QUERY = 10
TOTAL_REQUESTS = len(QUERIES) * REQUESTS_PER_QUERY

# 服务配置（统一20秒超时）
SERVICE_CONFIG = {
    'thordata': {
        'enabled': True,
        'concurrency': 20,
        'timeout': 20.0,
        'endpoint': 'http://170.106.180.18:9004/spiders',
        'method': 'GET',
        'headers': {"Content-Type": "application/json"},
        'auth': None,
        'payload_gen': lambda q: None
    },
    'brightdata': {
        'enabled': False,
        'concurrency': 20,
        'timeout': 20.0,
        'endpoint': 'https://api.brightdata.com/request',
        'method': 'POST',
        'headers': {
            "Authorization": "Bearer 6a6a016b625e8f60b5ebfa672e3e5380dc6ea8a52006a51bedc773ddef491386",
            "Content-Type": "application/json"
        },
        'auth': None,
        'payload_gen': lambda q: {
            "zone": "serp_api1",
            "url": f"https://www.google.com/search?q={q}",
            "format": "json"
        }
    },
    'oxylabs': {
        'enabled': False,
        'concurrency': 20,
        'timeout': 20.0,
        'endpoint': 'https://realtime.oxylabs.io/v1/queries',
        'method': 'POST',
        'headers': {"Content-Type": "application/json"},
        'auth': ('micky_YY5P7', 'Kmr10230304+'),
        'payload_gen': lambda q: {'source': 'google_search', 'query': q}
    },
    'serpapi_abcproxy': {
        'enabled': False,
        'concurrency': 20,
        'timeout': 20.0,
        'endpoint': 'https://serpapi.abcproxy.com/search',
        'method': 'GET',
        'headers': {},
        'auth': None,
        'payload_gen': lambda q: {
            "engine": "google",
            "q": q,
            "no_cache": "false",
            "api_key": "8cee5b9b782b602cdcafb2b3c23ac514"
        }
    }
}


# 统一请求处理器
def make_request(config, query):
    try:
        args = {
            'method': config['method'],
            'url': config['endpoint'],
            'headers': config['headers'],
            'timeout': config['timeout'],
            'auth': config['auth']
        }

        if config['method'] == 'GET':
            args['params'] = config['payload_gen'](query)
        else:
            args['json'] = config['payload_gen'](query)

        start_time = time.time()
        response = requests.request(**args)
        elapsed = (time.time() - start_time) * 1000

        success = response.status_code == 200 and len(response.content) >= 15 * 1024
        return elapsed, success, None

    except requests.exceptions.Timeout:
        return config['timeout'] * 1000, False, 'Timeout'  # 修正变量名
    except Exception as e:
        return 0, False, str(e)


# 结果收集器
class ResultCollector:
    def __init__(self):
        self.stats = {service: {'total': 0, 'success': 0, 'details': []}
                      for service in SERVICE_CONFIG}

    def add_result(self, service, query, req_num, elapsed, success, error):
        self.stats[service]['total'] += 1
        self.stats[service]['success'] += int(success)
        self.stats[service]['details'].append({
            'query': query,
            'req_num': req_num,
            'success': success,
            'time': f"{elapsed:.2f}ms" if success else f"Timeout({SERVICE_CONFIG['timeout']}s)",
            'error': error
        })


# 主程序
def main():
    collector = ResultCollector()

    with Progress() as progress:
        # 初始化进度条
        tasks = {
            service: progress.add_task(
                f"[cyan]{service}...",
                total=TOTAL_REQUESTS,
                visible=config['enabled']
            )
            for service, config in SERVICE_CONFIG.items()
        }

        # 遍历所有服务
        for service, config in SERVICE_CONFIG.items():
            if not config['enabled']:
                continue

            # 创建线程池
            with concurrent.futures.ThreadPoolExecutor(max_workers=config['concurrency']) as executor:
                futures = []

                # 提交所有请求任务
                for query in QUERIES:
                    for req_num in range(1, REQUESTS_PER_QUERY + 1):
                        futures.append(executor.submit(
                            make_request, config, query
                        ))

                # 处理结果
                for idx, future in enumerate(concurrent.futures.as_completed(futures)):
                    progress.update(tasks[service], advance=1)
                    try:
                        elapsed, success, error = future.result()
                        query = QUERIES[idx // REQUESTS_PER_QUERY]
                        req_num = (idx % REQUESTS_PER_QUERY) + 1

                        collector.add_result(service, query, req_num, elapsed, success, error)
                    except Exception as e:
                        print(f"结果处理异常: {str(e)}")

    # 生成报告
    for service in SERVICE_CONFIG:
        if not SERVICE_CONFIG[service]['enabled']:
            continue

        stats = collector.stats[service]
        success_rate = stats['success'] / stats['total'] if stats['total'] else 0

        print(f"\n{service} 统计:")
        print(f"总请求: {stats['total']}")
        print(f"成功率: {success_rate:.2%}")

        # 生成CSV报告
        with open(f"{service}_report.csv", "w", newline="", encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["关键词", "请求序号", "成功", "响应时间", "错误信息"])
            for detail in stats['details']:
                writer.writerow([
                    detail['query'],
                    detail['req_num'],
                    "成功" if detail['success'] else "失败",
                    detail['time'],
                    detail['error'] or ""
                ])


if __name__ == "__main__":
    main()