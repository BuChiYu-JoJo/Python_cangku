import aiohttp
import asyncio
import csv
import aiofiles
import json
from asyncio import Semaphore

AUTH_TOKEN = "5d7caa7f1e33019f9b1851e179415bc9"
CONCURRENT_LIMIT = 10  # 可手动配置并发数
INPUT_CSV = "url.csv"
OUTPUT_CSV = "result.csv"

sem = Semaphore(CONCURRENT_LIMIT)


async def read_urls(file_path):
    urls = []
    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
        content = await f.read()
        lines = content.splitlines()
        reader = csv.DictReader(lines)
        for row in reader:
            url = row.get("url", "").strip()
            if url:
                urls.append(url)
    return urls


async def fetch(session, url):
    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = json.dumps({"url": [url]})

    async with sem:
        try:
            async with session.post("https://scraperapi.thordata.com/builder?prodect_id=3", data=payload, headers=headers) as resp:
                status = resp.status
                text = await resp.text()
                try:
                    data = json.loads(text)
                    task_id = data.get("data", {}).get("task_id", "N/A")
                except Exception:
                    task_id = "ParseError"
                print(f"[{status}] {url} -> task_id: {task_id}")
                return [url, status, task_id]
        except Exception as e:
            print(f"[ERROR] {url} -> {e}")
            return [url, "ERROR", str(e)]


def write_results(results, file_path):
    with open(file_path, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['url', 'status_code', 'task_id'])
        writer.writerows(results)


async def main():
    urls = await read_urls(INPUT_CSV)
    results = []

    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        for future in asyncio.as_completed(tasks):
            result = await future
            results.append(result)

    write_results(results, OUTPUT_CSV)
    print(f"\n✅ 所有任务完成，结果已保存到 {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
