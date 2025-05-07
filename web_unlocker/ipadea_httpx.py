import asyncio
import httpx
import aiofiles
import csv
import os
import time
import re
import chardet
import traceback
from datetime import datetime
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROXY_USER = "zfhUUU111-zone-unblock-region-us"
PROXY_PASS = "zfh123321"
PROXY_HOST = "unblock.ipidea.net:17611"
PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}"

REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Referer": "https://www.walmart.com/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "X-Render-Type": "html"
}

URL_FILES = ["url_Lowes.csv"]

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', '_', name).strip()

async def log_error(detail_message, base_dir):
    os.makedirs(base_dir, exist_ok=True)
    log_path = os.path.join(base_dir, 'error.log')
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {detail_message}\n"
    async with aiofiles.open(log_path, 'a', encoding='utf-8') as f:
        await f.write(full_message)

async def fetch_url(client, semaphore, url_info, output_dir, results, base_dir):
    elapsed_time = 0.0
    simple_error_message = ""
    success = False
    file_size_bytes = 0
    filename = ""

    try:
        async with semaphore:
            request_start = time.time()
            response = await client.get(url_info['url'], headers=REQUEST_HEADERS)
            content = response.content
            elapsed_time = time.time() - request_start

            if response.status_code == 200:
                filename = f"{sanitize_filename(url_info['type'])}_{url_info['original_index']}_{url_info['request_seq']}.html"
                file_path = os.path.join(output_dir, filename)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                async with aiofiles.open(file_path, 'wb') as f:
                    await f.write(content)
                file_size_bytes = os.path.getsize(file_path)
                success = file_size_bytes >= 10240
                print(f"✅ 保存成功: {filename} ({file_size_bytes / 1024:.2f}KB) | 总耗时: {elapsed_time:.2f}s")
            else:
                simple_error_message = f"HTTP {response.status_code}"
                await log_error(
                    f"非200状态码: {response.status_code}, URL: {url_info['url']}, 返回内容: {response.text[:500]}",
                    base_dir
                )
                print(f"❌ 响应异常 [{url_info['url']}] | 状态码: {response.status_code} | 耗时: {elapsed_time:.2f}s")

    except Exception as e:
        elapsed_time = time.time() - request_start
        simple_error_message = f"异常: {type(e).__name__}"
        error_detail = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        await log_error(
            f"请求异常: {error_detail}, URL: {url_info['url']}",
            base_dir
        )
        print(f"⚠️ 请求失败 [{url_info['url']} #{url_info['request_seq']}] | 错误: {simple_error_message} - {str(e)} | 耗时: {elapsed_time:.2f}s")

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
    output_dir = os.path.join(base_subdir, f"concurrency_{concurrency}")
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

    async with httpx.AsyncClient(proxy=PROXY_URL, timeout=60.0, follow_redirects=True, verify=False) as client:
        await asyncio.gather(*[
            fetch_url(client, semaphore, task_info, output_dir, global_results, base_subdir)
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

    base_dir = "ipidea"
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
                f'"{row["目标网站"]}",{row["请求次数"]},{row["成功率"]},"{row["实际解锁"]}",' \
                f'{row["访问时间"]},"{row["生成文件"]}","{row["备注"]}",' \
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
