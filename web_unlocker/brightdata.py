import asyncio
import aiohttp
import aiofiles
import csv
import os
import time
import re
import chardet
from datetime import datetime
import sys

# ✅ 配置多个 URL 文件路径（不需从命令行传参）
URL_FILES = [
    "url_indeed.csv",
    "url_Instagram.csv",
    "url_Lowes.csv",
    "url_Safeway.csv",
    "url_walmart.csv"
]

# 请求参数配置
BASE_URL = "https://api.brightdata.com/request"
HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "PostmanRuntime-ApipostRuntime/1.1.0",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Authorization": "Bearer dae21b252b1b1998bf709f7c02b8f867a1bc573460ea387731512bdb33e3da0b"
}

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', '_', name).strip()

async def log_error(message, base_dir):
    os.makedirs(base_dir, exist_ok=True)
    log_file = os.path.join(base_dir, 'error.log')
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] {message}\n"
    async with aiofiles.open(log_file, 'a', encoding='utf-8') as f:
        await f.write(log_line)

async def fetch_url(session, semaphore, url_info, output_dir, results, base_dir):
    elapsed_time = 0.0
    simple_error_message = ""
    success = False
    file_size_bytes = 0
    filename = ""

    payload = {
        "url": url_info['url'],
        "zone": "web_unlocker1",
        "format": "raw",
        "data_format": "html"
    }

    try:
        async with semaphore:
            request_start = time.time()
            try:
                async with session.post(BASE_URL, json=payload, headers=HEADERS) as response:
                    content = await response.read()
                    elapsed_time = time.time() - request_start

                    if response.status == 200:
                        filename = f"{sanitize_filename(url_info['type'])}_{url_info['original_index']}_{url_info['request_seq']}.html"
                        file_path = os.path.join(output_dir, filename)
                        os.makedirs(os.path.dirname(file_path), exist_ok=True)
                        async with aiofiles.open(file_path, 'wb') as f:
                            await f.write(content)
                        file_size_bytes = os.path.getsize(file_path)
                        success = file_size_bytes >= 10240
                        print(f"✅ 保存成功: {filename} ({file_size_bytes / 1024:.2f}KB) | 总耗时: {elapsed_time:.2f}s")
                    else:
                        resp_text = await response.text()
                        simple_error_message = f"HTTP {response.status}"
                        await log_error(f"非200状态码: {response.status}, URL: {url_info['url']}, 返回: {resp_text[:300]}", base_dir)
                        print(f"❌ 状态码异常 [{url_info['url']}] | 状态: {response.status} | 耗时: {elapsed_time:.2f}s")
            except Exception as e:
                elapsed_time = time.time() - request_start
                simple_error_message = f"异常: {type(e).__name__}"
                await log_error(f"请求异常: {str(e)}, URL: {url_info['url']}", base_dir)
                print(f"⚠️ 请求失败 [{url_info['url']} #{url_info['request_seq']}] | 错误: {str(e)} | 耗时: {elapsed_time:.2f}s")
    except Exception as e:
        simple_error_message = f"异常: {type(e).__name__}"
        await log_error(f"请求外层异常: {str(e)}, URL: {url_info['url']}", base_dir)

    results.append({
        "目标网站": url_info['url'],
        "请求次数": 1,
        "成功率": 1 if success else 0,
        "实际解锁": "成功" if success else "失败",
        "访问时间": round(elapsed_time, 2),
        "生成文件": filename if success else "",
        "备注": simple_error_message,
        "并发数": url_info['concurrency'],
        "文件大小(KB)": round(file_size_bytes / 1024, 2)
    })

async def process_concurrency(concurrency, url_infos, global_results, base_subdir):
    output_dir = os.path.join(base_subdir, f'concurrency_{concurrency}')
    os.makedirs(output_dir, exist_ok=True)
    semaphore = asyncio.Semaphore(concurrency)

    tasks = []
    for idx, info in enumerate(url_infos, 1):
        for seq in range(1, concurrency + 1):
            task_info = {
                **info,
                "original_index": idx,
                "request_seq": seq,
                "concurrency": concurrency
            }
            tasks.append(task_info)

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[
            fetch_url(session, semaphore, task_info, output_dir, global_results, base_subdir)
            for task_info in tasks
        ])

def read_urls(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        encoding = chardet.detect(raw_data)['encoding'] or 'utf-8'
        if encoding.lower() in ['gb2312', 'gbk']:
            encoding = 'gb18030'

    with open(file_path, 'r', encoding=encoding, errors='replace') as f:
        reader = csv.DictReader(f)
        return [
            {"url": row['url'], "type": row.get('类型', 'default')}
            for row in reader
        ]

async def process_csv(url_file):
    url_infos = read_urls(url_file)
    global_results = []

    base_dir = "brightdata"
    base_subdir = os.path.join(base_dir, sanitize_filename(os.path.splitext(os.path.basename(url_file))[0]))
    os.makedirs(base_subdir, exist_ok=True)

    for concurrency in [1, 5, 10, 20]:
        print(f"\n== [{url_file}] 并发级别: {concurrency} ==")
        start = time.time()
        await process_concurrency(concurrency, url_infos, global_results, base_subdir)
        print(f"✅ 并发 {concurrency} 完成，用时: {time.time() - start:.2f} 秒")

    csv_path = os.path.join(base_subdir, 'global_results.csv')
    async with aiofiles.open(csv_path, 'w', encoding='utf-8-sig') as f:
        header = "目标网站,请求次数,成功率,实际解锁,访问时间,生成文件,备注,并发数,文件大小(KB)\n"
        await f.write(header)
        for row in global_results:
            line = (
                f'"{row["目标网站"]}",{row["请求次数"]},{row["成功率"]},"{row["实际解锁"]}",'
                f'{row["访问时间"]},"{row["生成文件"]}","{row["备注"]}",'
                f'{row["并发数"]},{row["文件大小(KB)"]}\n'
            )
            await f.write(line)

    print(f"\n📁 [{url_file}] 结果已保存到: {csv_path}")

async def main():
    for url_file in URL_FILES:
        if not os.path.isfile(url_file):
            print(f"⚠️ 文件不存在: {url_file}")
            continue
        await process_csv(url_file)

    print("\n✅ 所有并发测试已完成 ✅")

if __name__ == "__main__":
    asyncio.run(main())
