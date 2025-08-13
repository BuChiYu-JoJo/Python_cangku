import aiohttp
import asyncio
import csv
import os
import time
import logging
import random
from urllib.parse import quote
from datetime import datetime
from collections import deque

# ========= 配置 =========
SEARCH_ENGINES = {
    # 搜索引擎名称与对应的主域名
    "google": "www.google.com",
    "bing": "www.bing.com",
    "yandex": "yandex.com",
    "duckduckgo": "duckduckgo.com"
}
SEARCH_TERMS = [
    # 关键词列表（每分钟每个引擎将从中随机选择）
    "Apple", "Bread", "Cheese", "Salmon", "Chocolate",
    "Spinach", "Yogurt", "Pasta", "Almond", "Eggplant"
]

CONCURRENCY = 10  # 并发请求数上限
TIMEOUT_SECONDS = 30  # 单次请求的超时时间（秒）
MONITOR_DURATION_MINUTES = 99999999999999  # 总共监控的分钟数

# 告警规则阈值设置
THRESHOLD_SUCCESS_RATE = 95  # 成功率告警阈值（%）
THRESHOLD_TIMEOUT_RATE = 10  # 超时率告警阈值（%）
TIMEOUT_LIMIT = 10  # 请求耗时超过该值（秒）视为超时

DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=e05a6891a68b9b3b1cfacf8dcf5852bf647457439261362fac0a1e096951bfa9"  # 钉钉通知的Webhook地址
DINGTALK_KEYWORD = "SERP_Test_监控"  # 钉钉通知标题关键词

AUTH_HEADER = {
    # 请求头认证信息（ScraperAPI 授权令牌）
    "Authorization": "Bearer 5d7caa7f1e33019f9b1851e179415bc9",
    "Content-Type": "application/json"
}

# ========= 日志 =========
os.makedirs("logs", exist_ok=True)
main_logger = logging.getLogger("main")
main_logger.setLevel(logging.INFO)
fh = logging.FileHandler("monitor.log", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
main_logger.addHandler(fh)

def get_logger(engine, ts_folder):
    os.makedirs(f"logs/{engine}", exist_ok=True)
    logger = logging.getLogger(f"{engine}_{ts_folder}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(f"logs/{engine}/{ts_folder}.log", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(fh)
    return logger

# ========= 钉钉通知 =========
async def send_alert(title, content):
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": f"### {title}\n\n{content}"
        }
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(DINGTALK_WEBHOOK, json=payload) as resp:
                main_logger.info(f"钉钉通知状态: {resp.status}")
    except Exception as e:
        main_logger.error(f"钉钉发送异常: {e}")

# ========= 构建请求 =========
def build_payload(engine, term):
    domain = SEARCH_ENGINES[engine]
    return {
        "url": f"https://{domain}/search?q={quote(term)}",
        "json": "2"
    }

# ========= 单次请求 =========
async def fetch(session, engine, term):
    start = time.time()
    try:
        async with session.post(
            url="https://scraperapi.thordata.com/request",
            headers=AUTH_HEADER,
            json=build_payload(engine, term),
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
        ) as r:
            content = await r.text()
            elapsed = time.time() - start
            return {
                "engine": engine,
                "term": term,
                "status": r.status,
                "elapsed": elapsed,
                "is_timeout": elapsed > TIMEOUT_LIMIT,
                "success": (r.status == 200 and "html" not in r.headers.get("Content-Type", "")),
                "content_size": len(content.encode("utf-8")) / 1024
            }
    except Exception as e:
        return {
            "engine": engine,
            "term": term,
            "status": 0,
            "elapsed": time.time() - start,
            "is_timeout": True,
            "success": False,
            "exception": str(e),
            "content_size": 0
        }

# ========= 保存CSV =========
def save_csv(engine, ts_folder, rows):
    folder = f"csv/{engine}"
    os.makedirs(folder, exist_ok=True)
    file = os.path.join(folder, f"{ts_folder}.csv")
    with open(file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

# ========= 监控逻辑 =========
async def monitor():
    ts_folder = datetime.now().strftime("%Y%m%d_%H%M%S")

    status_window = {
        engine: {
            "success_rate": deque(maxlen=3),
            "timeout_rate": deque(maxlen=3),
            "recent_5xx": deque(maxlen=3),
            "csv_data": [],
            "logger": get_logger(engine, ts_folder)
        } for engine in SEARCH_ENGINES
    }

    for minute in range(MONITOR_DURATION_MINUTES):
        main_logger.info(f"\n===== 第 {minute + 1} 分钟监控 =====")
        async with aiohttp.ClientSession() as session:
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = []

            for engine in SEARCH_ENGINES:
                for _ in range(5):
                    term = random.choice(SEARCH_TERMS)
                    async def task(engine=engine, term=term):
                        async with sem:
                            r = await fetch(session, engine, term)
                            s = status_window[engine]
                            s["csv_data"].append({"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), **r})
                            log = s["logger"]
                            log.info(f"[{term}] 状态: {r['status']} 成功: {r['success']} 耗时: {r['elapsed']:.2f}s 大小: {r['content_size']:.2f}KB")
                            s["recent_5xx"].append(1 if r["status"] in (500, 502, 504) else 0)
                            return r
                    tasks.append(task())

            results = await asyncio.gather(*tasks)

        # 分析与预警
        from collections import defaultdict
        grouped = defaultdict(list)
        for r in results:
            grouped[r["engine"]].append(r)

        for engine in SEARCH_ENGINES:
            group = grouped[engine]
            total = len(group)
            if total == 0:
                continue
            success = sum(1 for r in group if r["success"])
            timeout = sum(1 for r in group if r["is_timeout"])
            sr = round(success / total * 100, 2)
            tr = round(timeout / total * 100, 2)

            s = status_window[engine]
            s["success_rate"].append(sr)
            s["timeout_rate"].append(tr)

            if len(s["success_rate"]) == 3 and all(x < THRESHOLD_SUCCESS_RATE for x in s["success_rate"]):
                await send_alert(f"{engine.upper()} 成功率告警", f"连续3分钟成功率低于 {THRESHOLD_SUCCESS_RATE}%：{list(s['success_rate'])}")

            if len(s["timeout_rate"]) == 3 and all(x > THRESHOLD_TIMEOUT_RATE for x in s["timeout_rate"]):
                await send_alert(f"{engine.upper()} 超时率告警", f"连续3分钟超时率高于 {THRESHOLD_TIMEOUT_RATE}%：{list(s['timeout_rate'])}")

            if sum(s["recent_5xx"]) >= 3:
                await send_alert(f"{engine.upper()} 5xx错误告警", "连续3次出现5xx错误，立即预警")
                s["recent_5xx"].clear()

        await asyncio.sleep(60)

    # 保存csv
    for engine in SEARCH_ENGINES:
        save_csv(engine, ts_folder, status_window[engine]["csv_data"])

# ========= 启动 =========
if __name__ == "__main__":
    asyncio.run(monitor())
