import requests
import concurrent.futures
import csv
import time
import os
from urllib.parse import quote
from requests.exceptions import Timeout, RequestException

# ====================
# **核心配置区**（必改部分）
# ====================
SEARCH_TERMS = [  # 显式配置检索词（支持中文/特殊字符）
    "Apple​",
    "​Bread​",
    "Cheese​",
    "Salmon​",
    "Chocolate",
    "Spinach​",
    "Yogurt​",
    "​Pasta​",
    "​Almond​",
    "​Eggplant​"
]  # 10个检索词（符合需求）

# 新增：服务启用控制配置
SERVICES_ENABLED = {              # 启用的服务（True/False）
    'thordata': False,
    'brightdata': True,
    'oxylabs': False,
    'serpapi': False,
    'yibiaopan': False # 新增服务启用开关
}

SERVICE_CONFIG = {  # 请替换为真实服务密钥
    'thordata': {
        'url': "http://170.106.180.18:9004/spiders",
        'headers': {"Content-Type": "application/json"}
    },
    'brightdata': {
        'url': "https://api.brightdata.com/request",
        'headers': {"Authorization": "Bearer 0589abd75f5c73032acb47491ac0693f59ca11ae9dfd6fc36b4390b9ca3109be"},
        'payload': lambda q: {  # 完全匹配curl参数
            "zone": "serp_api1",  # 保留用户指定的zone
            "url": f"https://www.google.com/search?q={quote(q)}",  # 直接使用原始拼接（不额外编码，与curl一致）
            "format": "raw"  # 匹配curl中的raw格式
        }
    },
    'oxylabs': {
        'url': "https://realtime.oxylabs.io/v1/queries",
        'auth': ("YOUR_OXYLABS_USER", "YOUR_OXYLABS_PASS"),  # 必改！
        'payload': lambda q: {"source": "google_search", "query": q}
    },
    'yibiaopan': {  # 修正后的yibiaopan服务配置
        'url': "https://scraperapi.thordata.com/request",
        'headers': {
            "Content-Type": "application/json",
            "Authorization": "Bearer 32c62e27a8884eaa7f44252965b46140"  # 真实密钥
        },
        'payload': lambda q: {"url": f"https://www.google.com/search?q={q}"}  # 移除多余的&json=1参数
    },
    'serpapi': {
        'url': "https://serpapi.abcproxy.com/search",
        'params': lambda q: {"engine": "google", "q": q, "api_key": "YOUR_SERPAPI_KEY"}  # 必改！
    }
}

# 基础配置（可调整）
PER_QUERY_REQUESTS = 100    # 每个词请求次数
TIMEOUT_SECONDS = 20        # 超时时间
MIN_FILE_SIZE_KB = 10       # 最小文件大小
CONCURRENCY = 50             # 建议并发数（避免服务商限流）
SAVE_EMPTY_RESPONSE = False  # 是否保存空响应（调试时设为True）

# ====================
# 工具函数（含内容校验）
# ====================
def save_response(service, term, req_num, content):
    """安全保存响应内容，自动处理空内容"""
    folder = f"data/{service}"
    os.makedirs(folder, exist_ok=True)
    filename = f"{term[:20].replace('/', '_')}_req{req_num:03d}.txt"  # 截断过长检索词
    path = os.path.join(folder, filename)
    
    if not content and not SAVE_EMPTY_RESPONSE:
        return None  # 不保存空文件
    
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    return path

# ====================
# 增强型请求处理（含5层校验）
# ====================
def handle_request(service, term, req_num):
    """
    5层校验确保有效响应：
    1. 状态码200
    2. 内容非空
    3. 非空白内容
    4. 文件大小达标
    5. 服务特定校验（如JSON格式）
    """
    config = SERVICE_CONFIG[service]
    start = time.time()
    try:
        # 构建请求
        if service == 'thordata':
            encoded_url = quote(f"https://www.google.com/search?q={term}", safe=':/?=')
            response = requests.get(
                f"{config['url']}?search_url={encoded_url}",
                headers=config['headers'],
                timeout=TIMEOUT_SECONDS
            )
        elif service == 'brightdata':
            response = requests.post(
                config['url'],
                headers=config['headers'],
                json=config['payload'](term),
                timeout=TIMEOUT_SECONDS
            )
        elif service == 'yibiaopan':
            response = requests.post(
                config['url'],
                headers=config['headers'],
                json=config['payload'](term),  # 正确构造URL
                timeout=TIMEOUT_SECONDS
            )
        elif service == 'oxylabs':
            response = requests.post(
                config['url'],
                auth=config['auth'],
                json=config['payload'](term),
                timeout=TIMEOUT_SECONDS
            )
        elif service == 'serpapi':
            response = requests.get(
                config['url'],
                params=config['params'](term),
                timeout=TIMEOUT_SECONDS
            )
        else:
            raise ValueError("未知服务")

        elapsed = (time.time() - start) * 1000
        content = response.text.strip()  # 去除首尾空白

        # 校验1：状态码
        if response.status_code != 200:
            return elapsed, False, f"状态码{response.status_code}"
        
        # 校验2：内容非空
        if not content:
            return elapsed, False, "响应内容为空"
        
        # 校验3：非空白内容
        if content.isspace():
            return elapsed, False, "内容全为空白字符"
        

        # 保存文件
        file_path = save_response(service, term, req_num, content)
        if not file_path:
            return elapsed, False, "空响应未保存"

        # 校验4：文件大小
        file_size = os.path.getsize(file_path)
        if file_size < MIN_FILE_SIZE_KB * 1024:
            return elapsed, False, f"文件大小不足({file_size/1024:.2f}KB)"

        return elapsed, True, "成功"

    except Timeout:
        return TIMEOUT_SECONDS * 1000, False, "请求超时"
    except RequestException as e:
        return (time.time() - start) * 1000, False, f"网络错误: {str(e)}"
    except Exception as e:
        return 0, False, f"致命错误: {str(e)}"

# ====================
# 统计与报告（简化版，去除汇总统计）
# ====================
def generate_report(service, stats):
    """生成带调试信息的CSV报告（仅详细记录）"""
    os.makedirs("reports", exist_ok=True)
    filename = f"reports/{service}_report_{time.strftime('%Y%m%d%H%M%S')}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            '时间戳', '服务', '检索词', '请求号', 
            '是否成功', '耗时(ms)', '错误原因', '文件路径', '文件大小(KB)'
        ])
        writer.writeheader()

        for item in stats['requests']:
            writer.writerow({
                '时间戳': item['timestamp'],
                '服务': service,
                '检索词': item['term'],
                '请求号': item['req_num'],
                '是否成功': "✔️" if item['success'] else "❌",
                '耗时(ms)': f"{item['elapsed']:.2f}",
                '错误原因': item['error'],
                '文件路径': item['file_path'] or "-",
                '文件大小(KB)': f"{item['file_size']:.2f}" if item['file_size'] else "-"
            })
    
    return filename

# ====================
# 主流程（带服务启用控制）
# ====================
def main():
    print("=== 爬虫请求验证工具 ===")
    print(f"检索词: {len(SEARCH_TERMS)}个 | 每个词请求: {PER_QUERY_REQUESTS}次")
    print("注意：请先替换SERVICE_CONFIG中的真实密钥！\n")

    for service in SERVICE_CONFIG:
        if not SERVICES_ENABLED.get(service, False):  # 新增服务启用检查
            print(f"跳过禁用服务: {service}")
            continue

        service_stats = {
            'requests': []  # 简化统计，仅记录请求详情
        }

        print(f"▶ 开始处理服务: {service}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = {}
            for term_idx, term in enumerate(SEARCH_TERMS, 1):
                for req_idx in range(1, PER_QUERY_REQUESTS+1):
                    future = executor.submit(handle_request, service, term, req_idx)
                    futures[future] = (term, req_idx)

            for future in concurrent.futures.as_completed(futures):
                term, req_idx = futures[future]
                elapsed, success, error = future.result()
                
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                file_path = None
                file_size = 0
                
                if success:
                    file_path = save_response(service, term, req_idx, "")
                    if file_path:
                        file_size = os.path.getsize(file_path) / 1024

                # 记录请求详情（去除汇总统计相关字段）
                service_stats['requests'].append({
                    'timestamp': timestamp,
                    'term': term,
                    'req_num': req_idx,
                    'success': success,
                    'elapsed': elapsed,
                    'error': error,
                    'file_path': file_path,
                    'file_size': file_size
                })

                # 实时进度（每50请求）
                if (term_idx * PER_QUERY_REQUESTS + req_idx) % 50 == 0:
                    print(f"进度: {term_idx}/{len(SEARCH_TERMS)} | 请求: {req_idx}/{PER_QUERY_REQUESTS}")

        # 生成报告（仅详细记录，无汇总统计）
        report_file = generate_report(service, service_stats)
        print(f"\n📋 {service} 报告已生成: {report_file}\n")

if __name__ == "__main__":
    main()