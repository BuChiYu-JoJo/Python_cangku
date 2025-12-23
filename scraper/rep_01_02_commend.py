# -*- coding: utf-8 -*-
# Time : 2025/02/25


import os
import sys
import asyncio
import json
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime, timezone, date as date_now

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)


from parsel import Selector
import re
import time
from projects.save.test_cos_fen import save_to_local_temp_file, upload_temp_file_to_cos, upload_xlsx_to_cos
from projects.tordata_uitls import requests_url_post
from copy import deepcopy
from projects.utils import get_ua
from projects.scraper_01_youtube.rep_01_get_page import get_page_content, get_page_content_comment, get_id
from projects.scraper_01_youtube.json_01_youtube_commend import youtube_extract_json_commend
from projects.scraper_01_youtube.json_01_youtube_detail import youtube_extract_json_detail
from projects.tordata_uitls import get_dongtai_proxy

youtube_headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=0, i",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
}


# 传入的参数处理
async def handle_parameter(user_inputs, error):
    print("user_inputs", user_inputs)
    spider_inputs_list = []
    spider_id = str(user_inputs['spider_id'])
    uesr_parms = user_inputs['spider_parameters']
    spider_errors = user_inputs.get('spider_errors', "")

    if spider_errors == "true" or spider_errors == "True":
        spider_errors = True

    for user_parm in uesr_parms:
        print("用户 传进来的参数==>：", user_parm)
        new_spider_parm = {}

        video_id = user_parm.get("video_id", "")
        domain = user_parm.get("domain", "")
        if video_id:
            if domain:
                url = f"{domain}/watch?v={video_id}"
            else:
                url = f"https://www.youtube.com/watch?v={video_id}"
        else:
            url = ""
        new_spider_parm['url'] = url
        try:
            page_turning = int(user_parm.get("page_turning", 1))
            if page_turning < 1:
                page_turning = 1
        except:
            page_turning = 1

        spider_inputs = {
            "user_input": user_parm,
            "spider_input": {},
            "discovery_input": {}
        }

        try:
            load_replies = int(user_parm.get("load_replies", "0"))
        except:
            load_replies = 0

        spider_inputs['spider_input']['spider_errors'] = spider_errors
        spider_inputs['spider_input']['proxy_region'] = user_parm.get("proxy_region", "us")
        spider_inputs['spider_input']['load_replies'] = load_replies
        spider_inputs['spider_input']['url'] = new_spider_parm['url']
        spider_inputs['spider_input']['video_id'] = video_id
        spider_inputs['spider_input']['page_turning'] = page_turning
        spider_inputs['spider_input']['user_input_id'] = spider_id
        error_str, spider_url, spider_item = await requests_url_post(new_spider_parm['url'])
        spider_inputs['spider_input']['spider_item'] = spider_item
        spider_inputs['spider_input']['spider_url'] = new_spider_parm['url']
        spider_inputs_list.append(spider_inputs)

    return spider_inputs_list, error


# 获取正文
async def get_youtube_page_content(spider_inputs, cookies=None, impersonate=True, retry_times=5):
    headers = deepcopy(youtube_headers)
    headers['user-agent'] = get_ua()

    user_input_id = spider_inputs['spider_input']['user_input_id']
    try:
        page_turning = int(spider_inputs['spider_input']['page_turning'])
    except:
        page_turning = 1
    print(user_input_id)
    print("spider_inputs 这里==>", spider_inputs)

    impersonate = False
    url = spider_inputs['spider_input']['spider_url']
    print("请求详情页url==>", url)

    load_replies = spider_inputs['spider_input']['load_replies']

    proxies = get_dongtai_proxy(validity=15, proxy_region="us")

    rest_item_ = await get_page_content(url, spider_inputs, method="GET", cookies=cookies, impersonate=impersonate, retry_times=retry_times, proxies=proxies)
    rest_item = await youtube_extract_json_detail(rest_item_)

    json = await get_id(rest_item_.get("resp_data", " "), spider_inputs, method="GET", cookies=None, impersonate=impersonate, retry_times=5, proxy_region="us", proxies=proxies)
    conmmend_url = "https://www.youtube.com/youtubei/v1/next?prettyPrint=False"

    conmmend_lits = []
    count = 0
    while True:
        count += 1
        print(f"开始爬取评论第 {count} 页")
        conmmend_item = await get_page_content_comment(conmmend_url, spider_inputs, method="POST", json=json, cookies=cookies, impersonate=impersonate, retry_times=retry_times, proxies=proxies)
        conmmend_item = await youtube_extract_json_commend(conmmend_item, rest_item, json, load_replies=load_replies)
        if conmmend_item:
            conmmend_lits += conmmend_item
            continuation = conmmend_item[0].get("continuation", "")
            if not continuation:
                break
            json['continuation'] = continuation
        else:
            break
        if count >= page_turning:
            break

    return conmmend_lits


async def youtube_multiple_resp(user_inputs, cos_key, error, retry_times=5):
    start_time = time.time()
    spider_inputs_list, error = await handle_parameter(user_inputs, error)

    spider_err = user_inputs.get('spider_errors', "")

    error_number = 0

    batch_size = 10
    result_num = 0
    size_in_bytes = 0
    # 依次按 batch_size 分组
    for batch_idx in range(0, len(spider_inputs_list), batch_size):
        batch = spider_inputs_list[batch_idx: batch_idx + batch_size]
        # 为本组启动所有协程
        tasks = [asyncio.create_task(get_youtube_page_content(si, retry_times=retry_times)) for si in batch]
        # 并发执行
        results = await asyncio.gather(*tasks)

        response_data_list = []
        nwe_co = []
        for rest in results:
            nwe_co = nwe_co + rest
        results = nwe_co
        print("提取的数量：", len(results))

        for html_result in results:
            result_num+=1
            if spider_err:
                html_result['err_stats'] = True
            result_data = html_result
            # 取值
            result_success = result_data.get("success", False)
            if not result_success:
                error_number += 1
            else:
                del result_data['success']

            if result_data.get("error", ""):
                error_str = result_data.get("error", "")
                error_code = result_data.get("error_code", 520)
                error.add(f"{error_code} {error_str}")

            # print("result_data: ", result_data)
            response_data_list.append(result_data)
        json_data = json.dumps(response_data_list)
        size_in_bytes += sys.getsizeof(json_data)
        await save_to_local_temp_file(cos_key + ".csv", response_data_list)
        await save_to_local_temp_file(cos_key + ".json", response_data_list)

    # print(results)
    try:
        if error_number == 0 and result_num == 0:
            error_number = len(user_inputs['spider_parameters'])
            result_num = len(user_inputs['spider_parameters'])
    except:
        error_number = 0

    end_time = time.time()
    total_time = end_time - start_time
    return result_num, total_time, size_in_bytes, error_number, error



async def youtube_multiple_results_commend(user_inputs, cos_key, error, retry_times=5):
    # 处理数据
    len_result, total_time, size_in_bytes, error_number, error = await youtube_multiple_resp(user_inputs, cos_key, error, retry_times=retry_times)
    print("提取的数量：", len_result)

    if len_result:
        error_rate = round(error_number / len_result, 2)
    else:
        error_rate = round(0 / 1, 2)
    print("数量： ", len_result)
    total_number = len_result

    await upload_xlsx_to_cos(cos_key + ".xlsx", cos_key + ".json")
    await upload_temp_file_to_cos(cos_key + ".csv")
    await upload_temp_file_to_cos(cos_key + ".json")
    return total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error


async def main():
    success_number = 0
    url_list = [
        "https://www.youtube.com/watch?v=bJTjJtRPqYE",
    ]

    for url in url_list:

        url_item = {'user_input': {'url': url},
                    'spider_input': {'spider_errors': True, 'url': url, 'user_input_id': '1.1', 'spider_item': {},
                                     'spider_url': url, "page_turning": ""},
                    'discovery_input': {}}


        url_rest = await get_youtube_page_content(url_item, cookies=None, impersonate=True, retry_times=5)
        print(url_rest)

    if len(url_list) == 0:
        print("URL 列表为空")
        return



if __name__ == '__main__':
    asyncio.run(main())