import requests
import concurrent.futures
import csv
import time
import os
from urllib.parse import quote
from rich.live import Live
from rich.table import Table
from rich.progress import Progress

# 配置参数
QUERIES = [
    "cat", "sushi", "burger", "pasta", "salad",
    "steak", "taco", "ramen", "curry", "sandwich"
]  # 10个查询关键词
REQUESTS_PER_QUERY = 100  # 每个关键词请求次数
TOTAL_REQUESTS = len(QUERIES) * REQUESTS_PER_QUERY  # 1000次总请求

# 服务配置（可在此处启用/禁用服务）
SERVICE_CONFIG = {
    'thordata': {
        'enabled': True,
        'concurrency': 50,
        'endpoint': 'http://170.106.180.18:9004/spiders',
        'headers': {"Content-Type": "application/json"},
        'auth': None
    },
    'brightdata': {
        'enabled': False,
        'concurrency': 20,
        'endpoint': 'https://api.brightdata.com/request',
        'headers': {
            "Authorization": "Bearer 6a6a016b625e8f60b5ebfa672e3e5380dc6ea8a52006a51bedc773ddef491386",
            "Content-Type": "application/json"
        },
        'auth': None
    },
    'oxylabs': {
        'enabled': False,
        'concurrency': 20,
        'endpoint': 'https://realtime.oxylabs.io/v1/queries',
        'headers': {"Content-Type": "application/json"},
        'auth': ('micky_YY5P7', 'Kmr10230304+')
    },
    'serpapi_abcproxy': {
        'enabled': False,
        'concurrency': 20,
        'endpoint': 'https://serpapi.abcproxy.com/search',
        'headers': {},
        'auth': None
    }
}

# 请求处理函数
def make_thordata_request(config, query, request_num):
    search_url = f"https://www.google.com/search?q={quote(query)}"
    encoded_url = quote(search_url, safe=':/?=')
    request_url = f"{config['endpoint']}?search_url={encoded_url}"
    
    start_time = time.time()
    try:
        response = requests.get(request_url, headers=config['headers'])
        elapsed = (time.time() - start_time) * 1000
        success = response.status_code == 200 and len(response.content) >= 15 * 1024
        return elapsed, success, query, request_num
    except Exception as e:
        return (time.time()-start_time)*1000, False, query, request_num

def make_brightdata_request(config, query, request_num):
    payload = {
        "zone": "serp_api1",
        "url": f"https://www.google.com/search?q={query}",
        "format": "json"
    }
    start_time = time.time()
    try:
        response = requests.post(
            config['endpoint'],
            headers=config['headers'],
            json=payload
        )
        elapsed = (time.time() - start_time) * 1000
        success = response.status_code == 200 and len(response.content) >= 15 * 1024
        return elapsed, success, query, request_num
    except Exception as e:
        return (time.time()-start_time)*1000, False, query, request_num

def make_oxylabs_request(config, query, request_num):
    payload = {'source': 'google_search', 'query': query}
    start_time = time.time()
    try:
        response = requests.post(
            config['endpoint'],
            auth=config['auth'],
            headers=config['headers'],
            json=payload
        )
        elapsed = (time.time() - start_time) * 1000
        success = response.status_code == 200 and len(response.content) >= 15 * 1024
        return elapsed, success, query, request_num
    except Exception as e:
        return (time.time()-start_time)*1000, False, query, request_num

def make_serpapi_request(config, query, request_num):
    params = {
        "engine": "google",
        "q": query,
        "no_cache": "false",
        "api_key": "8cee5b9b782b602cdcafb2b3c23ac514"
    }
    start_time = time.time()
    try:
        response = requests.get(
            config['endpoint'],
            params=params,
            headers=config['headers']
        )
        elapsed = (time.time() - start_time) * 1000
        success = response.status_code == 200 and len(response.content) >= 15 * 1024
        return elapsed, success, query, request_num
    except Exception as e:
        return (time.time()-start_time)*1000, False, query, request_num

# 结果处理器
class ResultCollector:
    def __init__(self):
        self.stats = {
            service: {
                'total': 0,
                'success': 0,
                'times': [],
                'details': []
            } for service in SERVICE_CONFIG
        }
    
    def add_result(self, service, elapsed, success, query, req_num):
        self.stats[service]['total'] += 1
        self.stats[service]['success'] += int(success)
        if success:
            self.stats[service]['times'].append(elapsed)
        self.stats[service]['details'].append(
            (query, req_num, success, f"{elapsed:.2f}ms" if success else "N/A")
        )

# 主程序
def main():
    collector = ResultCollector()
    request_handlers = {
        'thordata': make_thordata_request,
        'brightdata': make_brightdata_request,
        'oxylabs': make_oxylabs_request,
        'serpapi_abcproxy': make_serpapi_request
    }

    # 进度跟踪
    with Progress() as progress:
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

            handler = request_handlers[service]
            
            # 使用线程池处理请求
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=config['concurrency']
            ) as executor:
                futures = []
                
                # 提交所有请求任务
                for query in QUERIES:
                    for req_num in range(REQUESTS_PER_QUERY):
                        futures.append(
                            executor.submit(
                                handler,
                                config,
                                query,
                                req_num+1
                            )
                        )

                # 处理完成结果
                for future in concurrent.futures.as_completed(futures):
                    try:
                        elapsed, success, query, req_num = future.result()
                        collector.add_result(service, elapsed, success, query, req_num)
                        progress.update(tasks[service], advance=1)
                    except Exception as e:
                        print(f"请求异常: {str(e)}")

    # 生成报告
    for service in SERVICE_CONFIG:
        if not SERVICE_CONFIG[service]['enabled']:
            continue
        
        stats = collector.stats[service]
        success_rate = stats['success']/stats['total'] if stats['total'] else 0
        avg_time = sum(stats['times'])/len(stats['times']) if stats['times'] else 0
        
        print(f"\n{service} 统计结果:")
        print(f"总请求: {stats['total']}")
        print(f"成功数: {stats['success']} ({success_rate:.2%})")
        print(f"平均响应: {avg_time:.2f}ms")

        # 生成CSV文件
        with open(f"{service}_report.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Keyword", "Request#", "Success", "ResponseTime"])
            for detail in stats['details']:
                writer.writerow([
                    detail[0], 
                    detail[1],
                    "Y" if detail[2] else "N",
                    detail[3]
                ])
            writer.writerow([])
            writer.writerow(["Total Requests", stats['total']])
            writer.writerow(["Success Rate", f"{success_rate:.2%}"])
            writer.writerow(["Average Time", f"{avg_time:.2f}ms"])

if __name__ == "__main__":
    main()