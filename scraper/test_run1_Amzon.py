# -*- coding: utf-8 -*-
import asyncio
import datetime
import os
import sys


BASE = os.path.abspath(__file__)
while True:
    BASE = os.path.dirname(BASE)
    if BASE.endswith("thor-serpapi-backend"):
        break
sys.path.append(BASE)


from projects.project_01_tordata.spider_09_yamaxun_fen_version.rep_09_3_6_amazon.rep_09_3_6_amazon_1_2 import amazon_multiple_resp

async def main():
    # ==== build cos_key ====
    current_date = datetime.datetime.now()
    task_id = "task_003"
    data_dict = {"product_name": "thordata"}
    cos_key = "scrapers/{}/{}/{}".format(
        data_dict.get("product_name", ""),
        current_date.strftime("%Y/%m/%d"),
        task_id
    )

    # ==== build user_inputs for amazon ====
    spider_inputs = {
        "spider_errors": True,
        "spider_id": "amazon_global-product_by-keywords-brand",
        "spider_parameters": [
            {
                "keyword": "Coffee",
                "brands": "Starbucks",
                "domain": "www.amazon.com",  
                "page_turning": 1
            }
        ]
    }

    error = set()

    # ==== call ====
    result_num, total_time, size_in_bytes, error_number, error = await amazon_multiple_resp(
        spider_inputs,
        cos_key,
        error
    )

    print("Result statistics:")
    print("  total items:", result_num)
    print("  elapsed sec:", total_time)
    print("  bytes size :", size_in_bytes)
    print("  errors     :", error_number)
    print("  error set  :", error)

if __name__ == "__main__":
    asyncio.run(main())
