import requests
import concurrent.futures
import csv
import time
import os
from urllib.parse import quote

# 定义一个函数来生成 thordata 的 payload（不必要）
"""
def generate_thordata_payload(query):
    return {
        'source': 'google_search',
        'query': query,
        'context': [
            {'key': 'filter', 'value': 1}
        ]
    }
"""
# 定义一个函数来生成 brightdata 的 payload
def generate_brightdata_payload(query):
    return {
        "zone": "serp_api1",
        "url": f"https://www.google.com/search?q={query}",
        "format": "json"
    }

# 定义一个函数来生成 oxylabs 的 payload
def generate_oxylabs_payload(query):
    return {
        'source': 'google_search',
        'query': query,
#        'context': [
#            {'key': 'filter', 'value': 1}
#        ]
    }

# thordata 的 headers
thordata_headers = {
    "Content-Type": "application/json"
}

# brightdata 的 headers
brightdata_headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer 6a6a016b625e8f60b5ebfa672e3e5380dc6ea8a52006a51bedc773ddef491386"
}

# oxylabs 的 headers
oxylabs_headers = {
    "Content-Type": "application/json",
}

# 定义一个函数来处理 thordata 请求
def make_thordata_request(request_num, query):
    # 生成搜索 URL
    search_url = f"https://www.google.com/search?q={query}"
    # 对搜索 URL 进行编码
    encoded_search_url = quote(search_url, safe=':/?=')
    # 构建请求 URL
    request_url = f"http://170.106.180.18:9002/google?search_url={encoded_search_url}"
    start_time = time.time()
    try:
        response = requests.get(request_url)
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        # 检查响应内容
        try:
            response_data = response.json()
            if 'code' in response_data and response_data['code'] != 200:
                print(f"thordata 请求 {request_num} 失败，状态码: {response.status_code}，响应内容: {response.text}")
                return round(elapsed_time_ms, 2), False
        except ValueError:
            pass
        if response.status_code == 200 and response.text:
            # 创建 txt 文件夹
            if not os.path.exists('thordata'):
                os.makedirs('thordata')

            # 保存为 txt 文件，使用请求计次编号和查询关键词作为文件名一部分
            file_path = os.path.join('thordata', f'response_{query}_{request_num}.txt')
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(response.text)
            return round(elapsed_time_ms, 2), True
        else:
            print(f"thordata 请求 {request_num} 失败，状态码: {response.status_code}，响应内容: {response.text}")
            return round(elapsed_time_ms, 2), False
    except Exception as e:
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        print(f"thordata 请求 {request_num} 发生异常: {e}")
        return round(elapsed_time_ms, 2), False

# 定义一个函数来处理 brightdata 请求
def make_brightdata_request(request_num, query):
    payload = generate_brightdata_payload(query)
    start_time = time.time()
    try:
        response = requests.post(
            'https://api.brightdata.com/request',
            headers=brightdata_headers,
            json=payload
        )
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        # 检查响应状态码和内容
        if response.status_code == 200:
            # 创建 txt 文件夹
            if not os.path.exists('brightdata'):
                os.makedirs('brightdata')
            # 保存为 txt 文件
            file_path = os.path.join('brightdata', f'response_{query}_{request_num}.txt')
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(response.text)
            return round(elapsed_time_ms, 2), True
        else:
            # 明确处理502和其他非200状态码
            print(f"brightdata 请求 {request_num} 失败，状态码: {response.status_code}，响应内容: {response.text[:200]}")  # 限制日志长度
            return round(elapsed_time_ms, 2), False
    except Exception as e:
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        print(f"brightdata 请求 {request_num} 发生异常: {str(e)}")
        return round(elapsed_time_ms, 2), False

# 定义一个函数来处理 oxylabs 请求
def make_oxylabs_request(request_num, query):
    payload = generate_oxylabs_payload(query)
    start_time = time.time()
    try:
        response = requests.request(
            'POST',
            'https://realtime.oxylabs.io/v1/queries',
            auth=('micky_YY5P7', 'Kmr10230304+'),
            json=payload,
        )
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        # 检查响应内容
        if response.status_code == 200 and response.text:
            # 创建 txt 文件夹
            if not os.path.exists('oxylabs'):
                os.makedirs('oxylabs')

            # 保存为 txt 文件，使用请求计次编号和查询关键词作为文件名一部分
            file_path = os.path.join('oxylabs', f'response_{query}_{request_num}.txt')
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(response.text)
            return round(elapsed_time_ms, 2), True
        else:
            print(f"oxylabs 请求 {request_num} 失败，状态码: {response.status_code}，响应内容: {response.text}")
            return round(elapsed_time_ms, 2), False
    except Exception as e:
        end_time = time.time()
        elapsed_time_ms = (end_time - start_time) * 1000
        print(f"oxylabs 请求 {request_num} 发生异常: {e}")
        return round(elapsed_time_ms, 2), False

total_requests = 1000
# 可配置的并发数
thordata_concurrency = 20
brightdata_concurrency = 20
oxylabs_concurrency = 20

# 定义要查询的关键词列表
queries = ["pizza"]

# 存储所有服务的数据
all_services = ['thordata', 'brightdata', 'oxylabs']
all_service_times = {service: [] for service in all_services}
all_service_success_count = {service: 0 for service in all_services}
all_service_failure_count = {service: 0 for service in all_services}
all_service_total_time_ms = {service: 0 for service in all_services}
all_service_query_params = {service: [] for service in all_services}
all_service_request_success = {service: [] for service in all_services}

# 定义请求函数映射
request_functions = {
    'thordata': make_thordata_request,
    'brightdata': make_brightdata_request,
    'oxylabs': make_oxylabs_request
}

# 定义并发数映射
concurrency_map = {
    'thordata': thordata_concurrency,
    'brightdata': brightdata_concurrency,
    'oxylabs': oxylabs_concurrency
}

# 配置每个服务是否启用
service_enabled = {
    'thordata': True,
    'brightdata': False,
    'oxylabs': False
}

for service in all_services:
    if not service_enabled[service]:
        continue

    for query in queries:
        success_count = 0
        failure_count = 0
        total_time_ms = 0
        times = []
        request_success = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_map[service]) as executor:
            future_to_request = {executor.submit(request_functions[service], i, query): i for i in range(total_requests)}
            for future in concurrent.futures.as_completed(future_to_request):
                try:
                    elapsed_time_ms, success = future.result()
                    times.append(elapsed_time_ms)
                    all_service_query_params[service].append(query)
                    request_success.append(success)
                    all_service_request_success[service].append(success)
                    if success:
                        success_count += 1
                        total_time_ms += elapsed_time_ms
                    else:
                        failure_count += 1
                except Exception as exc:
                    print(f'{service} 产生异常: {exc}')

        all_service_times[service].extend(times)
        all_service_success_count[service] += success_count
        all_service_failure_count[service] += failure_count
        all_service_total_time_ms[service] += total_time_ms

        success_rate = success_count / total_requests
        failure_rate = failure_count / total_requests
        print(f"{service} 查询关键词: {query}")
        print(f"{service} 成功率: {success_rate}")
        print(f"{service} 失败率: {failure_rate}")
        print(f"{service} 总请求时间: {total_time_ms} ms")

    # 保存每个服务的数据到 CSV 文件
    with open(f'results_{service}.csv', 'w', newline='') as csvfile:
        fieldnames = ['统计类型', '请求参数', '访问是否成功', '访问成功率', '请求失败率', '请求时间(ms)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        # 新增总计行
        all_total_requests = total_requests * len(queries)
        writer.writerow({
            '统计类型': '总计',
            '请求参数': all_total_requests,
            '访问是否成功': '',
            '访问成功率': all_service_success_count[service] / all_total_requests,
            '请求失败率': all_service_failure_count[service] / all_total_requests,
            '请求时间(ms)': all_service_total_time_ms[service]
        })
        # 新增平均行
        all_average_time_ms = all_service_total_time_ms[service] / all_service_success_count[service] if all_service_success_count[service] > 0 else 0
        all_average_time_ms = round(all_average_time_ms, 2)
        writer.writerow({
            '统计类型': '平均',
            '请求参数': '',
            '访问是否成功': '',
            '访问成功率': all_service_success_count[service] / all_total_requests,
            '请求失败率': all_service_failure_count[service] / all_total_requests,
            '请求时间(ms)': all_average_time_ms
        })

        for t, q, success in zip(all_service_times[service], all_service_query_params[service], all_service_request_success[service]):
            success_str = "是" if success else "否"
            if not success:
                t = ''  # 请求失败时，请求时间置为空
            writer.writerow({
                '统计类型': '',
                '请求参数': q,
                '访问是否成功': success_str,
                '访问成功率': 1 if success else 0,
                '请求失败率': 0 if success else 1,
                '请求时间(ms)': t
            })