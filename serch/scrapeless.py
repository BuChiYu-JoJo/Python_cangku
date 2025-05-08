import requests
import csv
import os
import time
from urllib.parse import urlparse
import http.client
import json

http.client._MAXHEADERS = 1000
results = []

with open('url.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        target_url = row['url'].strip()
        error_message = ""
        success = False
        start_time = time.time()

        try:
            # 新的请求配置部分
            host = "api.scrapeless.com"
            url = f"https://{host}/api/v1/unlocker/request"
            token = "sk_xHkwC0y3aFeiG7JF8nf1WrmT0XcpWKhEvC2xhlJ0VgTWiSDcZ098F1GnzVIIhFNx"

            headers = {
                "x-api-token": token,
                "Content-Type": "application/json"
            }

            json_payload = json.dumps({
                "actor": "unlocker.webunlocker",
                "proxy": {"country": "ANY"},
                "input": {
                    "url": "https://google.com",
                    "method": "GET",
                    "redirect": True,
                    "headless": False,
                    "js_render": False,
                    "js_instructions": [
                        {"wait": 10000},
                        {"wait_for": [".dynamic-content", 30000]},
                        {"click": ["#load-more", 1000]},
                        {"fill": ["#search-input", "search term"]},
                        {"keyboard": ["press", "Enter"]},
                        {"evaluate": "window.scrollTo(0, document.body.scrollHeight)"},
                    ],
                    "block": {
                        "resources": [
                            "image",
                            "font",
                            "script",
                        ],  # "image", "font", "script"
                        "urls": ["https://example.com"]
                    }
                }
            })

            response = requests.post(url, headers=headers, data=json_payload)

            # 处理响应
            if response.status_code == 200:
                api_response = response.json()
                if "data" not in api_response or not api_response["data"]:
                    error_message = "API响应内容为空"
                    success = False
                else:
                    # 获取实际网页内容（根据API实际返回结构调整）
                    content = api_response["data"].get("content", "").encode()
                    if not content:
                        error_message = "响应内容为空"
                        success = False
                    else:
                        success = True
                        parsed_url = urlparse(target_url)
                        domain = parsed_url.netloc
                        path = parsed_url.path.lstrip('/')

                        filename = f"{domain}.html" if not path else f"{domain}_{path.replace('/', '_')[:100]}.html"
                        output_dir = 'brightdata'
                        os.makedirs(output_dir, exist_ok=True)
                        file_path = os.path.join(output_dir, filename)

                        try:
                            with open(file_path, 'wb') as f:
                                f.write(content)
                        except Exception as e:
                            error_message = f"文件保存失败：{e}（路径：{file_path}）"
                            success = False
            else:
                error_message = f"API状态码异常：{response.status_code}"

        except requests.exceptions.RequestException as e:
            error_message = f"请求失败：{e}"
        except KeyError as e:
            error_message = f"API响应格式错误：{e}"
        except Exception as e:
            error_message = f"未知错误：{e}（请检查 URL 格式）"

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)

        results.append({
            "目标网站": target_url,
            "请求次数": 1,
            "成功率": 1 if success else 0,
            "访问时间": elapsed_time,
            "备注": error_message[:200]
        })

with open('results_scrapeless.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=["目标网站", "请求次数", "成功率", "访问时间", "备注"])
    writer.writeheader()
    writer.writerows(results)

print(f"处理完成，共请求 {len(results)} 个 URL，结果保存到 results_scrapeless.csv")