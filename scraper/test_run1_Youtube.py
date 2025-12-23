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


from projects.scraper_01_youtube.youtube_multiple_results_product import youtube_multiple_results_product

async def main():
    # ==== build cos_key ====
    current_date = datetime.datetime.now()
    task_id = "task_youtube_111"
    data_dict = {"product_name": "thordata"}
    cos_key = "scrapers/{}/{}/{}".format(
        data_dict.get("product_name", ""),
        current_date.strftime("%Y/%m/%d"),
        task_id
    )

    # ==== build user_inputs for youtube====
    spider_inputs = {
        "spider_errors": True,
        "spider_id": "youtube_product_by-id",
        "spider_parameters": [
            {
                "video_id": "xBZb66tKH2Q"
            }
        ]
    }

    error = set()

    # ==== call ====
    result_num, total_time, size_in_bytes, error_number, error = await youtube_multiple_results_product(
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
