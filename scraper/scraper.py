import aiohttp
import asyncio
import csv
import json
from asyncio import Semaphore
from datetime import datetime

# 配置区
AUTH_TOKEN = "5d7caa7f1e33019f9b1851e179415bc9"
CONCURRENT_LIMIT = 1
REPEAT_COUNT = 100

# 请求任务配置（任务名 => 请求体）
TASKS = {
#    "1.1-ID3-Amazon通过URL收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/Vitamix-Blender-Professional-Grade-Container-Black/dp/B008H4SLV6?ref=dlx_deals_dg_dcl_B008H4SLV6_dt_sl14_9e_pi&pf_rd_r=F7PQP2WXJPT919418759&pf_rd_p=5741afe8-6913-401f-8c7f-c315ace7ff9e&th=1\"}],\"spider_id\":\"3\"},\"spider_name\":\"amazon.com\"}",
#    "1.2-ID366-Amazon-通过畅销商品URL收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/Best-Sellers-Tools-Home-Improvement-Kitchen-Bath-Fixtures/zgbs/hi/3754161/ref=zg_bs_unv_hi_2_680350011_1\",\"collect_child_categories\":\"\"},{\"url\":\"https://www.amazon.com/-/zh/gp/bestsellers/hi/?ie=UTF8&ref_=sv_hg_1\",\"collect_child_categories\":\"\"}],\"spider_id\":\"366\"},\"spider_name\":\"amazon.com\"}",
#    "1.3-ID353-Amazon-通过类别URL收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/s?i=specialty-aps&bbn=16225007011&rh=n%3A16225007011%2Cn%3A172456&language=zh&ref=nav_em__nav_desktop_sa_intl_computer_accessories_and_peripherals_0_2_7_2\",\"sort_by\":\"Newest Arrivals\"},{\"url\":\"https://www.amazon.com/s?i=baby-products-intl-ship\",\"sort_by\":\"Best Sellers\"}],\"spider_id\":\"353\"},\"spider_name\":\"amazon.com\"}",
    "1.4-ID352-Amazon-通过关键词收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"keyword\":\"Apple watch\"},{\"keyword\":\"coffce\"}],\"spider_id\":\"352\"},\"spider_name\":\"amazon.com\"}",
#    "1.5-ID367-Amazon-通过关键词收集产品信息": "{\"spider_info\":{\"spider_parameters\":[{\"upc\":\"841710108224\"},{\"upc\":\"840044714668\"}],\"spider_id\":\"367\"},\"spider_name\":\"amazon.com\"}",
#    "2.1-ID368-Amazon-通过URL收集产品评论": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/HISDERN-Checkered-Handkerchief-Classic-Necktie/dp/B0BRXPR726\"},{\"url\":\"https://www.amazon.com/LAURA-GELLER-NEW-YORK-Retractable/dp/B086H4VCBJ\"}],\"spider_id\":\"368\"},\"spider_name\":\"amazon.com\"}",
#    "3.1-ID377-Amazon-通过URL收集全球产品数据": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/dp/B0CHHSFMRL/\"},{\"url\":\"https://www.amazon.co.jp/X-TRAK-Folding-Bicycle-Carbon-Adjustable/dp/B0CWV9YTLV/ref=sr_1_1_sspa?crid=3MKZ2ALHSLFOM&dib=eyJ2IjoiMSJ9.YnBVPwJ7nLxlNGHktwDTFM5v2evnsXlnZTJHJKuG8dLeeRCILpy0Knr3ofiKpUGQYi6xR6y4tgdtal85DJ8u6DD_n9r1oVCXdVo0NFmNAfStU6E-MhBig5p_gZGjluAYv5HgUIoEPl0v3iMiRxZNRfivqB-utxOkPOOfXIBHLemry17XcltUDTQqtJv-kP-ZqdP29mjD2cRlbkALtHPKU44MvBC9WUrNcUHAMrlAxtTAByuriywMqz-w2P0HCeehcZTJ1EiLf2VR8cxCiwuaUbIOU3tr1kDN6D7yYPrgRn4.6AOdSmJsksZkqLg8kNM6EvWxIFOijCsP2zo5NLHn1P4&dib_tag=se&keywords=Bicycles&qid=1716973495&sprefix=%2Caps%2C851&sr=8-1-spons&sp_csd=d2lkZ2V0TmFtZT1zcF9hdGY&psc=1\"}],\"spider_id\":\"377\"},\"spider_name\":\"amazon.com\"}",
#    "3.2-ID376-Amazon-通过品牌收集全球产品数据": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/s?k=football&i=fashion&rh=n%3A7141123011%2Cp_123%3A233083\"},{\"url\":\"https://www.amazon.com/s?k=football+shoes&i=fashion&rh=n%3A7141123011%2Cp_123%3A6832\"}],\"spider_id\":\"376\"},\"spider_name\":\"amazon.com\"}",
#   "3.3-ID375-Amazon-通过类别URL收集全球产品数据": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/s?i=specialty-aps&bbn=16225007011&rh=n%3A16225007011%2Cn%3A172456&language=zh&ref=nav_em__nav_desktop_sa_intl_computer_accessories_and_peripherals_0_2_7_2\",\"sort_by\":\"Best Sellers\",\"get_sponsored\":\"true\"},{\"url\":\"https://www.amazon.de/-/en/b/?node=1981001031&ref_=Oct_d_odnav_d_355007011_2&pd_rd_w=OjE3S&content-id=amzn1.sym.0069bc39-a323-47d6-a8fb-7558e4a563e4&pf_rd_p=0069bc39-a323-47d6-a8fb-7558e4a563e4&pf_rd_r=6YXZ7HGFNNEAF0GSDPDH&pd_rd_wg=0yR1G&pd_rd_r=a95cb46c-78ef-4b7b-845d-49fe04556440\",\"sort_by\":\"Price: High to Low\",\"get_sponsored\":\"true\"}],\"spider_id\":\"375\"},\"spider_name\":\"amazon.com\"}",
#    "3.4-ID354-Amazon-通过关键词收集全球产品数据": "{\"spider_info\":{\"spider_parameters\":[{\"keyword\":\"ipad\",\"domain\":\"https://www.amazon.com\",\"page_turning\":\"1\"},{\"keyword\":\"watch\",\"domain\":\"https://www.amazon.com\",\"page_turning\":\"2\"}],\"spider_id\":\"354\"},\"spider_name\":\"amazon.com\"}",
#   "3.5-ID374-Amazon-通过卖家收集全球产品数据": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/stores/page/4750579C-0CF4-4BF9-B7E8-F782E52D7683?ingress=0&visitId=63ba151f-9b4f-4938-b6a3-fd23c249b72f\"},{\"url\":\"https://www.amazon.de/stores/page/05D3BFBD-322A-4A03-B09A-98E2BF02A729?ingress=0&visitId=342ff490-603a-49e5-bde7-37f1855a4fb5\"}],\"spider_id\":\"374\"},\"spider_name\":\"amazon.com\"}",
#    "4.1-ID369-Amazon-通过URL收集卖家信息": "{\"spider_info\":{\"spider_parameters\":[{\"url\":\"https://www.amazon.com/s?me=A3AZYNALJBV2WE&language=zh&marketplaceID=ATVPDKIKX0DER\"},{\"url\":\"https://www.amazon.com/-/zh/stores/Canon/page/5FDDA83E-27A1-472F-8444-4828B50C4243?is_byline_deeplink=true&deeplink=60D61B31-BD52-43D3-8CC0-46549C735D0A&redirect_store_id=5FDDA83E-27A1-472F-8444-4828B50C4243&lp_asin=B0D1DV6FV5&ref_=ast_bln&store_ref=bl_ast_dp_brandLogo_sto\"}],\"spider_id\":\"369\"},\"spider_name\":\"amazon.com\"}",
#    "5.1-ID370-Amazon-通过关键词和主域名收集产品列表": "{\"spider_info\":{\"spider_parameters\":[{\"keyword\":\"Switch\",\"domain\":\"https://www.amazon.com/\",\"page_turning\":\"1\"},{\"keyword\":\"PS5\",\"domain\":\"https://www.amazon.com/\",\"page_turning\":\"2\"}],\"spider_id\":\"370\"},\"spider_name\":\"amazon.com\"}"
}


sem = Semaphore(CONCURRENT_LIMIT)

def extract_spider_id(payload: str) -> str:
    try:
        data = json.loads(payload)
        return data["spider_info"]["spider_id"]
    except Exception:
        return "unknown"

async def fetch(session, task_name, payload):
    spider_id = extract_spider_id(payload)
    endpoint = f"https://scraperapi.thordata.com/builder?prodect_id={spider_id}"

    headers = {
        "Authorization": f"Bearer {AUTH_TOKEN}",
        "Content-Type": "application/json"
    }

    async with sem:
        try:
            start = datetime.now()
            async with session.post(endpoint, data=payload, headers=headers) as resp:
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

        print(f"[DEBUG] 获取到 {len(results)} 条结果")

        filename = f"{name.split('【')[0].strip()}.csv"
        with open(filename, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["任务名称", "状态码", "耗时秒", "返回内容"])
            for r in results:
                writer.writerow([r["任务名称"], r["状态码"], r["耗时秒"], r["返回内容"]])

        print(f"✅ 任务完成：{filename}")

if __name__ == "__main__":
    asyncio.run(main())
