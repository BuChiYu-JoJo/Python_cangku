import asyncio
import aiohttp
import aiofiles
import json
import os
import csv
import time
from datetime import datetime

MAX_CONCURRENT_REQUESTS = 55
TOTAL_REQUESTS = 275

AUTH = aiohttp.BasicAuth('thdata_apQ5r', 'mybkBa2Q+E+y8Fc')
URL = 'https://realtime.oxylabs.io/v1/queries'
PAYLOAD_TEMPLATE = {
    'source': 'google_search',
    'query': 'nike',
    'context': [{'key': 'filter', 'value': 1}]
}

RESULT_DIR = 'results'
CSV_LOG = 'results_summary.csv'
ERROR_LOG = 'error.log'
os.makedirs(RESULT_DIR, exist_ok=True)

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
result_rows = []

async def save_json(data, filename):
    async with aiofiles.open(filename, mode='w', encoding='utf-8') as f:
        await f.write(json.dumps(data, ensure_ascii=False, indent=2))

async def log_error(index, error_msg):
    async with aiofiles.open(ERROR_LOG, mode='a', encoding='utf-8') as f:
        await f.write(f"[{datetime.now()}] Request {index}: {error_msg}\n")

async def fetch(session, index):
    async with semaphore:
        query = f"{PAYLOAD_TEMPLATE['query']}_{index}"
        payload = dict(PAYLOAD_TEMPLATE)
        payload['query'] = query

        result = {
            '查询词': query,
            '请求状态': '失败',
            '状态码': '',
            '保存文件名': '',
            '错误信息': '',
            '耗时（秒）': 0,
            '响应大小（KB）': 0
        }

        start_time = time.perf_counter()

        try:
            async with session.post(URL, json=payload, auth=AUTH, timeout=30) as resp:
                duration = time.perf_counter() - start_time
                result['状态码'] = resp.status
                result['耗时（秒）'] = round(duration, 3)

                content = await resp.read()
                result['响应大小（KB）'] = round(len(content) / 1024, 3)

                try:
                    json_data = json.loads(content)
                except json.JSONDecodeError:
                    json_data = {"error": "Invalid JSON response"}
                    result['错误信息'] = "Invalid JSON response"

                if resp.status == 200:
                    filename = os.path.join(RESULT_DIR, f"response_{index}.json")
                    await save_json(json_data, filename)
                    result['请求状态'] = '成功'
                    result['保存文件名'] = filename
                    print(f"[{index}] 成功: 保存至 {filename}")
                else:
                    error_msg = f"HTTP {resp.status}: {json_data}"
                    await log_error(index, error_msg)
                    result['错误信息'] = str(json_data)
                    print(f"[{index}] 错误: {error_msg}")

        except Exception as e:
            duration = time.perf_counter() - start_time
            result['耗时（秒）'] = round(duration, 3)
            err = repr(e)
            await log_error(index, err)
            result['错误信息'] = err
            print(f"[{index}] 异常: {err}")

        result_rows.append(result)

def write_csv_sync(rows):
    headers = ['查询词', '请求状态', '状态码', '保存文件名', '错误信息', '耗时（秒）', '响应大小（KB）']
    with open(CSV_LOG, mode='w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, i) for i in range(1, TOTAL_REQUESTS + 1)]
        await asyncio.gather(*tasks)
    write_csv_sync(result_rows)

if __name__ == '__main__':
    import sys

    # ✅ Windows 异步兼容补丁，防止 Proactor 报错
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())