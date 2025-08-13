import aiohttp
import asyncio
import time
import os
import logging
import csv
from datetime import datetime
from statistics import mean

# ========== 配置 ==========
URL_LIST = [
    "https://www.google.com",
    "https://www.bing.com",
    "https://www.wikipedia.org",
]

SERVICE_CONFIG = {
    "name": "thordata",
    "endpoint": "https://universalapi.thordata.com/request",
    "headers": {
        "Authorization": "Bearer ff3aaeb7605583f7e51675da4ad2a0de",
        "Content-Type": "application/json"
    },
    "json_payload": lambda url: {
        "url": url,
        "type": "html",
        "js_render": "True"
    }
}

CONCURRENCY = 10           #并发配置
TIMEOUT_SECONDS = 30       #每个请求最大等待时间
MIN_CONTENT_SIZE_KB = 10   #有效返回内容的最小大小（小于该值视为失败）
RUN_DURATION_SECONDS = 120 #单轮测试执行时长(秒)
SCHEDULE_INTERVAL = 300    #循环运行时间间隔(秒)
TOTAL_REPEAT = None  # 设置为 None 表示无限循环，直到手动停止程序

ENABLE_SAVE_RESPONSE = False  #是否保存成功的网页响应内容
ENABLE_CONSOLE_LOG = False    #是否在控制台打印日志信息
ENABLE_FILE_LOG = True        #是否将日志写入本地日志文件logs/run.log

DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=e05a6891a68b9b3b1cfacf8dcf5852bf647457439261362fac0a1e096951bfa9"
DINGTALK_KEYWORD = "test"

# ========== 日志配置 ==========
logger = logging.getLogger("unlocker")
logger.setLevel(logging.INFO)
if ENABLE_FILE_LOG:
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler("logs/run.log", mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(fh)


def log_print(msg):
    if ENABLE_CONSOLE_LOG:
        print(msg)
    if ENABLE_FILE_LOG:
        logger.info(msg)


# ========== 获取 URL 专属日志记录器 ==========
def get_url_logger(url: str, ts_folder: str):
    domain = url.split("//")[-1].split("/")[0].replace(".", "_")
    log_dir = f"logs/{domain}"
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(f"{domain}_{ts_folder}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        file_path = os.path.join(log_dir, f"{ts_folder}.log")
        handler = logging.FileHandler(file_path, mode="a", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        logger.addHandler(handler)
    return logger


# ========== 单次请求 ==========
async def fetch_once(session, semaphore, url, index, ts_folder):
    async with semaphore:
        payload = SERVICE_CONFIG["json_payload"](url)
        headers = SERVICE_CONFIG["headers"]
        endpoint = SERVICE_CONFIG["endpoint"]
        domain_logger = get_url_logger(url, ts_folder)

        result = {
            "url": url,
            "success": False,
            "status": None,
            "error": "",
            "elapsed_ms": 0,
            "content_size_kb": 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        start_time = time.time()
        try:
            async with session.post(endpoint, json=payload, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as resp:
                text = await resp.text()
                elapsed = (time.time() - start_time) * 1000
                result.update({
                    "status": resp.status,
                    "elapsed_ms": elapsed,
                    "content_size_kb": len(text.encode("utf-8")) / 1024
                })

                if resp.status != 200:
                    result["error"] = f"HTTP {resp.status}"
                    domain_logger.info(f"[{index}] ❌ HTTP 错误, 耗时={elapsed:.1f}ms")
                    return result

                if result["content_size_kb"] < MIN_CONTENT_SIZE_KB:
                    result["error"] = f"内容过小: {result['content_size_kb']:.2f}KB"
                    domain_logger.info(f"[{index}] ⚠ 内容过小, 耗时={elapsed:.1f}ms")
                    return result

                if ENABLE_SAVE_RESPONSE:
                    folder = f"responses/{ts_folder}"
                    os.makedirs(folder, exist_ok=True)
                    filename = f"{index}_{url.replace('https://', '').replace('/', '_')}.html"
                    with open(os.path.join(folder, filename), "w", encoding="utf-8") as f:
                        f.write(text)

                result["success"] = True
                domain_logger.info(f"[{index}] ✅ 成功, 耗时={elapsed:.1f}ms, 大小={result['content_size_kb']:.2f}KB")
                return result

        except asyncio.TimeoutError:
            elapsed = (time.time() - start_time) * 1000
            result["error"] = "请求超时"
            result["elapsed_ms"] = elapsed
            domain_logger.info(f"[{index}] ❌ 请求超时, 耗时={elapsed:.1f}ms")

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            result["error"] = f"异常: {str(e)}"
            result["elapsed_ms"] = elapsed
            domain_logger.info(f"[{index}] ❌ 异常: {e}, 耗时={elapsed:.1f}ms")

        return result


# ========== 钉钉推送 ==========
async def send_dingtalk(results):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"### Universal_Scraping_{DINGTALK_KEYWORD} 结果汇总 - {now}"]

    for url in URL_LIST:
        url_results = [r for r in results if r["url"] == url]
        total = len(url_results)
        success = [r for r in url_results if r["success"]]
        fail = total - len(success)
        success_rate = round(len(success) / total * 100, 2) if total else 0.0
        avg_time = round(mean(r["elapsed_ms"] for r in success), 2) if success else 0.0
        avg_size = round(mean(r["content_size_kb"] for r in success), 2) if success else 0.0

        lines.append(
            f"\n#### 解锁网站：{url}\n"
            f"- 并发数：{CONCURRENCY}\n"
            f"- 请求次数：{total}\n"
            f"- 成功：{len(success)}\n"
            f"- 失败：{fail}\n"
            f"- 成功率：{success_rate}%\n"
            f"- 平均耗时：{avg_time} ms\n"
            f"- 平均大小：{avg_size} KB\n"
        )

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"{DINGTALK_KEYWORD} 执行结果",
            "text": "\n".join(lines)
        }
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(DINGTALK_WEBHOOK, json=payload) as resp:
            if resp.status == 200:
                log_print("📢 钉钉通知已发送")
            else:
                log_print(f"❌ 钉钉通知失败: {resp.status}")

    log_print("📊 钉钉推送内容：\n" + "\n".join(lines))


# ========== 单轮测试 ==========
async def run_once():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    semaphore = asyncio.Semaphore(CONCURRENCY)
    results = []
    index = 0
    per_url_duration = RUN_DURATION_SECONDS / len(URL_LIST)

    async with aiohttp.ClientSession() as session:
        for url in URL_LIST:
            log_print(f"▶ 开始 URL: {url}（分配时长约 {per_url_duration:.1f}s）")
            end_time = time.time() + per_url_duration

            while time.time() < end_time:
                tasks = []
                for _ in range(CONCURRENCY):
                    index += 1
                    tasks.append(fetch_once(session, semaphore, url, index, timestamp))
                batch_results = await asyncio.gather(*tasks)
                results.extend(batch_results)

    os.makedirs("reports", exist_ok=True)
    csv_file = f"reports/result_{timestamp}.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    log_print(f"📁 结果已保存：{csv_file}")
    await send_dingtalk(results)


# ========== 循环调度 ==========
async def scheduled_loop():
    count = 0
    while TOTAL_REPEAT is None or count < TOTAL_REPEAT:
        log_print(f"\n🔁 开始第 {count + 1} 轮测试：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        await run_once()
        count += 1
        log_print(f"✅ 第 {count} 轮完成，等待 {SCHEDULE_INTERVAL} 秒...\n")
        await asyncio.sleep(SCHEDULE_INTERVAL)


# ========== 启动入口 ==========
if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(scheduled_loop())
