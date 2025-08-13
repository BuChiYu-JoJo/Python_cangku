import requests
import urllib3
import csv
import time
import os
from datetime import datetime

# 关闭 HTTPS 证书警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 设置代理
proxies = {
    'http': 'http://lightsong_2UizC:Zxs6412915==@unblock.oxylabs.io:60000',
    'https': 'https://lightsong_2UizC:Zxs6412915==@unblock.oxylabs.io:60000',
}

# 要测试的状态码
#status_codes = [
#    400, 401, 402, 403, 404, 405, 406, 407,
#    408, 409, 410, 411, 412, 413, 414, 415,
#    416, 417, 418, 422, 426, 429
#]

status_codes = [
    400, 403, 407, 408, 429
]

# 输出文件夹和 CSV 文件
response_dir = "responses"
csv_file = "result.csv"
os.makedirs(response_dir, exist_ok=True)

# CSV 表头
fieldnames = ["timestamp", "url", "status_code", "elapsed_seconds", "response_preview", "html_file"]

# 初始化 CSV 写入
with open(csv_file, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    # 遍历状态码并请求
    for code in status_codes:
        url = f"https://httpstat.us/{code}"
        print(f"Requesting: {url}")

        html_filename = f"{response_dir}/{code}.html"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            start_time = time.time()
            response = requests.get(url, proxies=proxies, verify=False, timeout=60)
#            response = requests.get(url, verify=False, timeout=60)
            elapsed = round(time.time() - start_time, 3)
            content = response.text.strip()
            preview = content.replace("\n", " ").replace("\r", "")[:500]
            status = response.status_code

            # 保存为 HTML 文件
            with open(html_filename, "w", encoding="utf-8") as html_file:
                html_file.write(content)

        except Exception as e:
            elapsed = round(time.time() - start_time, 3)
            content = f"[Error] {str(e)}"
            preview = content[:500]
            status = "Request Failed"
            html_filename = ""  # 没有保存成功则留空

        # 写入 CSV
        writer.writerow({
            "timestamp": timestamp,
            "url": url,
            "status_code": status,
            "elapsed_seconds": elapsed,
            "response_preview": preview,
            "html_file": html_filename
        })

print(f"\n✅ 所有请求完成，结果已写入 {csv_file}，响应已保存至 {response_dir}/")
