import aiohttp
import asyncio
import csv
import json
import aiofiles
from asyncio import Semaphore
from datetime import datetime

# 配置区
API_ENDPOINT = "https://scraperapi.thordata.com/builder?prodect_id=3"
AUTH_TOKEN = "5d7caa7f1e33019f9b1851e179415bc9"
CONCURRENT_LIMIT = 5        # 并发数
REPEAT_COUNT = 10           # 每个请求体执行次数

# 请求任务配置（任务名 => 请求体）
TASKS = {
    "Amazon - 通过 URL 收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/HISDERN-Checkered-Handkerchief-Classic-Necktie/dp/B0BRXPR726\"},{\"url\":\"https://www.amazon.com/LAURA-GELLER-NEW-YORK-Retractable/dp/B086H4VCBJ\"}],\"spider_id\":\"3\"},\"spider_name\":\"amazon.com\"}",
    "Amazon - 通过畅销商品 URL 收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/-/zh/lighting-ceiling-fans/b/?ie=UTF8&node=495224&ref_=sv_hg_5\":\"\"},{\"url\":\"https://www.amazon.com/Best-Sellers-Tools-Home-Improvement-Kitchen-Bath-Fixtures/zgbs/hi/3754161/ref=zg_bs_nav_hi_1\",\"collect_child_categories\":\"\"}],\"spider_id\":\"366\"},\"spider_name\":\"amazon.com\"}",
    "Amazon - 通过类别 URL 收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com\",\"sort_by\":\"Best Sellers\"}],\"spider_id\":\"353\"},\"spider_name\":\"amazon.com\"}",
    "Amazon - 通过关键词收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"keyword\":\"Apple Watch\"},{\"keyword\":\"Coffee\"}],\"spider_id\":\"352\"},\"spider_name\":\"amazon.com\"}",
    "Amazon - 通过 UPC 收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"upc\":\"841710108224\"},{\"upc\":\"840044714668\"}],\"spider_id\":\"367\"},\"spider_name\":\"amazon.com\"}",
    "Amazon - 通过 URL 收集产品评论": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/HISDERN-Checkered-Handkerchief-Classic-Necktie/dp/B0BRXPR726\"},{\"url\":\"https://www.amazon.com/LAURA-GELLER-NEW-YORK-Retractable/dp/B086H4VCBJ\"}],\"spider_id\":\"368\"},\"spider_name\":\"amazon.com\"}",
    "Amazon - 通过 URL 收集全球产品数据": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/dp/B0CHHSFMRL/\"},{\"url\":\"https://www.amazon.co.jp/X-TRAK-Folding-Bicycle-Carbon-Adjustable/dp/B0CWV9YTLV/ref=sr_1_1_sspa?...}],\"spider_id\":\"377\"},\"spider_name\":\"amazon.com\"}",
    "Amazon - 通过类别 URL 收集全球产品数据": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/s?i=luggage-intl-ship\",\"sort_by\":\"Best Sellers\",\"get_sponsored\":\"\"}],\"spider_id\":\"375\"},\"spider_name\":\"amazon.com\"}",
    "Amazon - 通过 URL 收集卖家信息": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/s?i=specialty-aps&bbn=16225019011&rh=n%3A7141123011%2Cn%3A16225019011%2Cn%3A1040658&ref=nav_em__nav_desktop_sa_intl_clothing_0_2_14_2\"}],\"spider_id\":\"369\"},\"spider_name\":\"amazon.com\"}",
}

sem = Semaphore(CONCURRENT_LIMIT)

async def fetch(session, task_name, payload):
    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    async with sem:
        try:
            start = datetime.now()
            async with session.post(API_ENDPOINT, data=payload, headers=headers) as resp:
                text = await resp.text()
                duration = (datetime.now() - start).total_seconds()
                return {
                    "任务名称": task_name,
                    "状态码": resp.status,
                    "耗时秒": round(duration, 2),
                    "返回内容": text
                }
        except Exception as e:
            return {
                "任务名称": task_name,
                "状态码": "ERROR",
                "耗时秒": 0,
                "返回内容": str(e)
            }

async def run_task(task_name, payload, repeat):
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, task_name, payload) for _ in range(repeat)]
        for coro in asyncio.as_completed(tasks):
            res = await coro
            results.append(res)
    return results

async def main():
    for name, payload in TASKS.items():
        print(f"\n🚀 开始任务：{name}（{REPEAT_COUNT}次）")
        results = await run_task(name, payload, REPEAT_COUNT)

        filename = f"{name.split('【')[0].strip()}.csv"
        async with aiofiles.open(filename, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.writer(await f.__aenter__())
            await writer.writerow(["任务名称", "状态码", "耗时秒", "返回内容"])
            for r in results:
                await writer.writerow([r["任务名称"], r["状态码"], r["耗时秒"], r["返回内容"]])

        print(f"✅ 任务完成：{filename}")

if __name__ == "__main__":
    asyncio.run(main())
