import requests
import concurrent.futures
import csv
import time
import os
from urllib.parse import quote
import warnings  # 新增警告处理库

# ====================
# **核心配置区**（必改部分）
# ====================
SEARCH_TERMS = [  
    "Apple", "Bread", "Cheese", "Salmon", "Chocolate",
    "Spinach", "Yogurt", "Pasta", "Almond", "Eggplant"
]

SERVICE_CONFIG = {  
    'thordata': {
        'proxy_user': "td-customer-serp_HhFgxd5euqcb2",  
        'proxy_pass': "JJtvFttulx7n",
        'proxy_host': "scraping.thordata.com",
        'proxy_port': "30001",
    }
}

PER_QUERY_REQUESTS = 1       
TIMEOUT_SECONDS = 20          
CONCURRENCY =   1           
SAVE_EMPTY_RESPONSE = False   

# ====================
# 工具函数（保留核心）
# ====================
def save_response(term, req_num, content, raw_response=None):  # 新增raw_response参数
    folder = "data/thordata"
    os.makedirs(folder, exist_ok=True)
    filename = f"{term[:20].replace('/', '_')}_req{req_num:03d}.txt"
    path = os.path.join(folder, filename)

    # 保存原始响应内容（用于问题复现）
    if raw_response:
        raw_path = os.path.join(folder, f"{os.path.splitext(filename)[0]}_raw.gz")
        with open(raw_path, 'wb') as f:
            f.write(raw_response)

    if not content and not SAVE_EMPTY_RESPONSE:
        return None
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    return path


# ====================
# **thordata 专用请求逻辑**
# ====================
def handle_request(term, req_num):
    config = SERVICE_CONFIG['thordata']
    start = time.time()

    try:
        proxy = f"http://{config['proxy_user']}:{config['proxy_pass']}@{config['proxy_host']}:{config['proxy_port']}"
        proxies = {'http': proxy, 'https': proxy}

        with requests.Session() as session:
            session.proxies = proxies
            session.verify = False  # 业务需求保留
            session.headers.update({  # 新增反反爬头部
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
                'Accept-Encoding': 'gzip, deflate'  # 显式声明支持的编码
            })
            
            url = f"https://www.google.com/search?q={quote(term, safe=':/?=')}"
            response = session.get(url, timeout=TIMEOUT_SECONDS, stream=True)  # 流式获取防止内存爆炸

            # 强制禁用自动解压（关键修复）
            if 'gzip' in response.headers.get('Content-Encoding', ''):
                raw_data = response.content  # 保存原始gzip数据
                try:
                    with gzip.GzipFile(fileobj=BytesIO(raw_data)) as f:
                        content = f.read().decode('utf-8', errors='replace')
                except (gzip.BadGzipFile, OSError) as e:
                    save_response(term, req_num, None, raw_data)  # 保存原始gzip文件用于分析
                    return (time.time()-start)*1000, False, f"gzip解压失败: {str(e)}"
            else:
                content = response.text.strip()

            elapsed = (time.time() - start) * 1000

            # 增强的内容校验
            if response.status_code != 200:
                return elapsed, False, f"状态码{response.status_code}"
            if not content or content.strip() in ['<html><head></head><body></body></html>', '']:
                return elapsed, False, "内容无效（空/空白页）"

            file_path = save_response(term, req_num, content, raw_response=raw_data if 'raw_data' in locals() else None)
            if not file_path:
                return elapsed, False, "空响应未保存"

            file_size = os.path.getsize(file_path)
            if file_size < 10 * 1024:
                return elapsed, False, f"文件过小({file_size/1024:.2f}KB)"

            return elapsed, True, "成功"

    except requests.exceptions.Timeout:
        return TIMEOUT_SECONDS * 1000, False, "请求超时"
    except requests.exceptions.RequestException as e:
        return (time.time() - start) * 1000, False, f"网络错误: {str(e)}"
    except Exception as e:  # 新增通用异常捕获
        return (time.time() - start) * 1000, False, f"未知错误: {str(e)}"

# ====================
# 统计与报告（极简版）
# ====================
def generate_report(stats):
    os.makedirs("reports", exist_ok=True)
    filename = f"reports/thordata_report_{time.strftime('%Y%m%d%H%M%S')}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['时间戳', '检索词', '请求号', '是否成功', '耗时(ms)', '错误原因'])
        writer.writeheader()
        for item in stats:
            writer.writerow({
                '时间戳': item['timestamp'],
                '检索词': item['term'],
                '请求号': item['req_num'],
                '是否成功': "✔️" if item['success'] else "❌",
                '耗时(ms)': f"{item['elapsed']:.2f}",
                '错误原因': item['error'] or "-"
            })
    return filename

# ====================
# 主流程（新增警告忽略逻辑）
# ====================
if __name__ == "__main__":
    # 消除InsecureRequestWarning警告（关键修改）
    warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
    
    print("=== thordata 爬虫验证工具 ===")
    print(f"检索词: {len(SEARCH_TERMS)}个 | 每个词请求: {PER_QUERY_REQUESTS}次\n")
    
    stats = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = {
            executor.submit(handle_request, term, req_num): (term, req_num)
            for term in SEARCH_TERMS
            for req_num in range(1, PER_QUERY_REQUESTS + 1)
        }
        
        for future in concurrent.futures.as_completed(futures):
            term, req_num = futures[future]
            elapsed, success, error = future.result()
            
            stats.append({
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'term': term,
                'req_num': req_num,
                'success': success,
                'elapsed': elapsed,
                'error': error
            })
            
            total = len(SEARCH_TERMS) * PER_QUERY_REQUESTS
            completed = len(stats)
            print(f"\r进度: {completed}/{total} ({completed/total*100:.1f}%)", end='')
    
    print("\n\n📋 报告生成中...")
    report_file = generate_report(stats)
    print(f"报告已保存: {report_file}")