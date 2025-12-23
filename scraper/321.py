# -*- coding: utf-8 -*-
# Time : 2025/02/25


import os
import sys
import json
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime, timezone, date as date_now

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

import re
import time
from projects.save.test_cos_fen import save_to_local_temp_file, upload_temp_file_to_cos, upload_temp_file_to_cos_2, upload_xlsx_to_cos
from projects.tordata_uitls import requests_url_post
import asyncio
import aiohttp
import random
import uuid
from yt_dlp import YoutubeDL
from projects.save.cos_util import CosUtil
from yt_dlp.utils import DownloadError
from projects.save.boto_util import DigitalOceanSpaces

youtube_headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=0, i",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
}


async def get_static_ip():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://staticip.apolloproxy.com/api/get_native_ip_list") as res:
                if res.status != 200:
                    print(f"代理获取失败，状态码：{res.status}")
                    return None

                result_data = await res.json()
                proxy_list = result_data["ret_data"]["list"]
                return proxy_list
        except Exception as e:
            print(f"代理获取失败，发生错误：{e}")
            return None

# 传入的参数处理
async def handle_parameter(user_inputs, error):
    print("user_inputs", user_inputs)
    spider_inputs_list = []
    spider_id = str(user_inputs['spider_id'])
    # uesr_parms = user_inputs['spider_parameters']
    uesr_parms = user_inputs['spider_info']
    spider_errors = user_inputs.get('spider_errors', "")
    proxy = user_inputs.get('proxy', "")

    if spider_errors == "true" or spider_errors == "True":
        spider_errors = True

    for user_parm in uesr_parms:
        print("用户 传进来的参数==>：", user_parm)
        new_spider_parm = {}

        spider_inputs = {
            "user_input": user_parm,
            "spider_input": {},
            "discovery_input": {}
        }

        video_id = user_parm.get("video_id", False)
        domain = user_parm.get("domain", "")
        if video_id:
            if domain:
                url = f"{domain}/watch?v={video_id}"
            else:
                url = f"https://www.youtube.com/watch?v={video_id}"
        else:
            url = ""
        new_spider_parm['url'] = url

        spider_inputs['spider_input']['spider_errors'] = spider_errors
        spider_inputs['spider_input']['proxy_region'] = user_parm.get("proxy_region", "us")
        spider_inputs['spider_input']['subtitles_language'] = user_parm.get("subtitles_language", "en")

        spider_inputs['spider_input']['url'] = new_spider_parm['url']
        spider_inputs['spider_input']['proxy'] = proxy
        spider_inputs['spider_input']['video_id'] = video_id

        spider_inputs['spider_input']['user_input_id'] = spider_id
        error_str, spider_url, spider_item = await requests_url_post(new_spider_parm['url'])
        spider_inputs['spider_input']['spider_item'] = spider_item
        # spider_inputs['spider_input']['spider_url'] = spider_url
        spider_inputs['spider_input']['spider_url'] = new_spider_parm['url']
        spider_inputs_list.append(spider_inputs)

    return spider_inputs_list, error


# 使用 asyncio.Queue 来传递文件名
async def my_hook(d, queue):
    if d['status'] == 'finished':
        await queue.put(d['filename'])

async def test_download(url, cos_key, proxy, subtitles_type, subtitles_language="en"):
    try:
        task_id = cos_key.split('/')[-1]
    except:
        return False, 0, "Other Errors - Other undefined errors.", 520, "", ""
    current_date = datetime.now()
    current_date.strftime("%Y/%m/%d")

    print(f"开始执行")
    # url = "https://www.youtube.com/watch?v=DTfndYXi4oM"  # 替换为你的视频链接
    parsed_url = urlparse(url)
    video_id = parse_qs(parsed_url.query).get("v", [None])[0]
    print(video_id)

    boto_key = cos_key
    other_param = f"{video_id}"
    if subtitles_language:
        other_param = other_param + f"_{subtitles_language}"
    if other_param:
        boto_key = boto_key + "/" + other_param

    if not subtitles_language:
        subtitles_language = "en"
    generated_uuid = str(uuid.uuid4).replace("-", "")
    print(f"当前使用代理 {proxy}")


    queue = asyncio.Queue()
    ydl_opts_srt = {
        "proxy": f"http://{proxy}",  # 代理地址
        "outtmpl": f"/opt/youtube_download/{task_id}/{other_param}.%(ext)s",  # 这里用 %(ext)s 生成合适的扩展名
        "noplaylist": True,  # 不下载播放列表
        "quiet": False,  # 是否静默
        "no_warnings": True,  # 不显示警告
        "writesubtitles": True,  # 启用字幕下载
        "subtitleslangs": [subtitles_language],  # 设置字幕语言
        "skip_download": True,  # 只下载字幕，不下载视频
        "convert-subs": "srt",  # 强制将字幕转换为 srt 格式
        'progress_hooks': [lambda d: asyncio.create_task(my_hook(d, queue))],
    }
    if subtitles_type == "auto_generated":
        ydl_opts_srt["writeautomaticsub"] = True
    try:
        with YoutubeDL(ydl_opts_srt) as ydl_srt:
            result = ydl_srt.download([url])
            if result == 0:
                downloaded_filename = await queue.get()
                print(f"下载成功, 文件路径为： {downloaded_filename}")
                try:
                    file_size_srt = os.path.getsize(downloaded_filename)
                except Exception as e:
                    print(f"获取size失败： {str(e)}")
                    file_size_srt = 0
                houzhui = downloaded_filename.split('.')[-1]
                file_name = f"{boto_key}.{houzhui}"
                # result = await upload_temp_file_to_cos_2(f"/opt/youtube_download/{other_param}.srt", file_name)
                result = await upload_temp_file_to_cos_2(downloaded_filename, file_name)
                if result:
                    return True, file_size_srt, "", "", file_name
                else:
                    print(f"上传失败")
                    return False, file_size_srt, "Internal Server Error - Internal server error.", 500, ""
    except DownloadError as e:
        # 这里你可以根据错误消息判断类型，比如网络错误、视频被删等
        error_msg = str(e)
        if "HTTP Error 429" in error_msg:
            error = "Internal Server Error - Internal server error."
            error_code = ""
        elif "unable to download video" in error_msg:
            error = "Not Found - The requested resource does not exist."
            error_code = 404
        elif "this video has been terminated." in error_msg:
            error = "Not Found - The requested resource does not exist."
            error_code = 404
        elif "looks truncated" in error_msg:
            error = "Not Found - The requested resource does not exist."
            error_code = 404
        else:
            error = "Other Errors - Other undefined errors."
            error_code = 520

        return False, 0, error, error_code, ""

    except Exception as e:
        return False, 0, "Other Errors - Other undefined errors.", 520, ""


# 获取正文
async def get_youtube_page_content(spider_inputs, cos_key, proxy_list, retry_times=5):
    url = spider_inputs['spider_input']['spider_url']
    video_id = spider_inputs['spider_input']['video_id']
    subtitles_type = spider_inputs['spider_input']['subtitles_type']
    subtitles_language = spider_inputs['spider_input'].get('subtitles_language', "en")
    proxy_ = spider_inputs['spider_input'].get('proxy', "")

    rest_item = {}
    rest_item['transcriptdownloadUrl'] = ""
    rest_item['video_id'] = video_id
    rest_item['file_size'] = 0
    rest_item['error'] = "Other Errors - Other undefined errors."
    rest_item['error_code'] = 520
    print("请求详情页url==>", url)
    for i in range(retry_times):
        proxy_count = len(proxy_list)
        print(f"当前代理数量{proxy_count}")
        n = random.randrange(proxy_count)
        print(f"当前随机数{n}")
        ipinfo = proxy_list[n]
        # proxy = f"static_monitor_2506:ttAaciYGtHV3CZkMUAsL@{ipinfo['ip']}:{6666}"
        print("proxy： ", proxy_)
        proxy = ""
        if proxy_:
            proxy = proxy_.replace("@:", f"@{ipinfo['ip']}:")
            # proxy = proxy.replace("@:", f"@{'156.228.9.2'}:")
        result, file_size, error, error_code, file_name = await test_download(url, cos_key, proxy, subtitles_type, subtitles_language=subtitles_language)
        try:
            new_file_size = round(file_size / 1024 / 1024, 2)
        except:
            new_file_size = 0
        if result:
            if file_name:
                # file_name = urljoin("https://nyc3.digitaloceanspaces.com/th-scrapers/", file_name)
                file_name = urljoin("https://data.thordata.com/", file_name)
            rest_item['transcriptdownloadUrl'] = file_name
            rest_item['success'] = True
            rest_item['file_size'] = new_file_size
            rest_item['file_size_flow'] = file_size
            rest_item['error'] = ""
            rest_item['error_code'] = ""
            break
        else:
            rest_item['file_size'] = new_file_size
            rest_item['file_size_flow'] = file_size
            rest_item['error'] = error
            rest_item['error_code'] = error_code
    return rest_item


async def youtube_multiple_resp(user_inputs, cos_key, error, retry_times=5):
    start_time = time.time()
    spider_inputs_list, error = await handle_parameter(user_inputs, error)

    spider_err = user_inputs.get('spider_errors', "")
    proxy_list = await get_static_ip()

    error_number = 0

    batch_size = 10
    result_num = 0
    size_in_bytes = 0
    video_link = []
    # 依次按 batch_size 分组
    for batch_idx in range(0, len(spider_inputs_list), batch_size):
        batch = spider_inputs_list[batch_idx: batch_idx + batch_size]
        # 为本组启动所有协程
        tasks = [asyncio.create_task(get_youtube_page_content(si, cos_key, proxy_list=proxy_list, retry_times=retry_times)) for si in batch]
        # 并发执行
        results = await asyncio.gather(*tasks)

        response_data_list = []
        for html_result in results:
            result_num+=1
            if spider_err:
                html_result['err_stats'] = True

            result_data = html_result
            # 取值
            result_success = result_data.get("success", False)
            file_size = result_data.get("file_size_flow", 0)
            size_in_bytes += file_size

            video_url = result_data.get("audiodownloadUrl")
            if video_url:
                video_link.append(video_url)

            if not result_success:
                error_number += 1
            else:
                del result_data['success']
            try:
                del result_data['file_size_flow']
            except:
                print("删除失败")
            if result_data.get("error", ""):
                error_str = result_data.get("error", "")
                error_code = result_data.get("error_code", 520)
                error.add(f"{error_code} {error_str}")
            response_data_list.append(result_data)

        await save_to_local_temp_file(cos_key + ".csv", response_data_list)
        await save_to_local_temp_file(cos_key + ".json", response_data_list)

    try:
        if error_number == 0 and result_num == 0:
            error_number = len(user_inputs['spider_parameters'])
            result_num = len(user_inputs['spider_parameters'])
    except:
        error_number = 0

    end_time = time.time()
    total_time = end_time - start_time
    return result_num, total_time, size_in_bytes, error_number, error, video_link



async def youtube_multiple_results_download_subtitle(user_inputs, cos_key, error, retry_times=5):
    # dos = DigitalOceanSpaces()

    # 处理数据
    len_result, total_time, size_in_bytes, error_number, error,video_link = await youtube_multiple_resp(user_inputs, cos_key, error, retry_times=retry_times)
    print("提取的数量：", len_result)

    if len_result:
        error_rate = round(error_number / len_result, 2)
    else:
        error_rate = round(0 / 1, 2)
    print("数量： ", len_result)
    total_number = len_result
    try:
        filename_id = cos_key.split("/")[-1]
    except:
        filename_id = cos_key
    filename = ROOT_DIR + "/temp_file/" + filename_id
    await upload_xlsx_to_cos(cos_key + "/" + filename_id + ".xlsx", filename + ".json")
    await upload_temp_file_to_cos_2(filename + ".csv", cos_key + "/" + filename_id + ".csv")
    await upload_temp_file_to_cos_2(filename + ".json", cos_key + "/" + filename_id + ".json")
    # await dos.insert_file(filename + ".csv", cos_key + "/" + filename_id + ".csv")
    # await dos.insert_file(filename + ".json", cos_key + "/" + filename_id + ".csv")
    return total_number, size_in_bytes, total_time, error_rate, error_number, len_result, error, video_link


async def main():
    success_number = 0
    url_list = [
        "https://www.youtube.com/watch?v=_SdpvpvVrLY",
    ]
    proxy_list = await get_static_ip()

    for url in url_list:

        url_item = {'user_input': {'url': url},
                    'spider_input': {'spider_errors': True, 'url': url, 'user_input_id': '1.1', 'spider_item': {},
                                     'spider_url': url, "audio_format": "m4a"},
                    'discovery_input': {}}


        url_rest = await get_youtube_page_content(url_item, proxy_list)
        print(url_rest)
        success = url_rest.get("success", False)

        if success:
            success_number += 1

    if len(url_list) == 0:
        print("URL 列表为空")
        return


if __name__ == '__main__':
    asyncio.run(main())