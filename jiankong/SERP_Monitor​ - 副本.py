import aiohttp
import asyncio
import csv
import os
import time
import logging
import traceback
import random
from urllib.parse import quote
from datetime import datetime
from collections import deque
from logging import FileHandler

# ========= 配置 =========
# 修改为结构化形式
SEARCH_ENGINES = {
    "google": {"domain": "www.google.com", "param": "q"},
    "bing": {"domain": "www.bing.com", "param": "q"},
    "yandex": {"domain": "yandex.com", "param": "text"},
    "duckduckgo": {"domain": "duckduckgo.com", "param": "q"},
}

SEARCH_TERMS = [
    # 关键词列表（每分钟每个引擎将从中随机选择）
    "Apple", "Bread", "Cheese", "Salmon", "Chocolate",
    "Spinach", "Yogurt", "Pasta", "Almond", "Eggplant",
    "Banana", "Blueberry", "Mango", "Broccoli", "Carrot",
    "Zucchini", "Chicken", "Tofu", "Shrimp", "Quinoa",
    "Rice", "Oatmeal", "Milk", "Cottage Cheese", "Soy Milk",
    "Walnut", "Pumpkin Seed", "Honey", "Avocado", "Dark Chocolate"
]

CONCURRENCY = 10  # 并发请求数上限
TIMEOUT_SECONDS = 10  # 单次请求的超时时间（秒）
MONITOR_DURATION_MINUTES = 9999999999999  # 总共监控的分钟数（建议设置足够大，程序可手动终止）

# 告警规则阈值设置
THRESHOLD_SUCCESS_RATE = 95  # 成功率告警阈值（%）
THRESHOLD_TIMEOUT_RATE = 10  # 超时率告警阈值（%）
TIMEOUT_LIMIT = 10  # 请求耗时超过该值（秒）视为超时
MIN_CONTENT_SIZE_KB = 2  # 小于 2KB 视为无效

DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=e05a6891a68b9b3b1cfacf8dcf5852bf647457439261362fac0a1e096951bfa9"  # 钉钉通知的Webhook地址
DINGTALK_KEYWORD = "test"  # 钉钉通知标题关键词

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

def get_logger(engine, _ts_folder=None):  # _ts_folder 保留参数不使用
    logger_name = f"{engine}_daily"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # 检查是否已有当天文件的 Handler，避免重复添加
    log_path = get_daily_log_path(engine)
    if not any(isinstance(h, FileHandler) and h.baseFilename == os.path.abspath(log_path) for h in logger.handlers):
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(fh)

    return logger

#========日志保存=============
def get_daily_log_path(engine):
    date_str = datetime.now().strftime("%Y%m%d")
    folder = f"logs/{engine}"
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{date_str}.log")

# ========= 钉钉通知 =========
async def send_alert(title, content, data_list=None, metric_label="指标值", engine=None):
    """
    发送钉钉告警
    :param title: 告警标题
    :param content: 告警内容
    :param data_list: 历史值列表
    :param metric_label: 指标名称（如“成功率”或“超时率”）
    """
    data_section = ""
    if data_list:
        data_section = f"<br><br>近3分钟{metric_label}：<br>{', '.join(str(x) + '%' for x in data_list)}"

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"{DINGTALK_KEYWORD} | {title}" + (f" | {engine.upper()}_{datetime.now().strftime('%H:%M:%S')}" if engine else ""),
            "text": f"### SERP_{DINGTALK_KEYWORD} | {title}<br><br>{content}{data_section}"
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
    config = SEARCH_ENGINES[engine]
    domain = config["domain"]
    param = config["param"]
    return {
        "url": f"https://{domain}/search?{param}={quote(term)}&json=1"
    }

# ========= 单次请求 =========
async def fetch(session, engine, term, logger):
    start = time.time()
    try:
        # 模拟返回 502 错误（调试用）
#        if random.random() < 0.6:
#            elapsed = time.time() - start
#            return {
#                "engine": engine,
#                "term": term,
#                "status": 502,
#                "elapsed": elapsed,
#                "is_timeout": False,
#                "success": False,
#                "content_size": 0
#            }

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
                "success": (r.status == 200 and "html" not in r.headers.get("Content-Type", "") and len(content.encode("utf-8")) / 1024 >= MIN_CONTENT_SIZE_KB),
                "content_size": len(content.encode("utf-8")) / 1024,
                "timestamp": time.time()  # 时间戳
            }
    except Exception as e:
        elapsed = time.time() - start
        err_type = type(e).__name__
        error_text = str(e)
        request_url = build_payload(engine, term)["url"]  # 获取 URL
#        tb_str = traceback.format_exc(limit=5)  # 可调整输出深度

        # 获取该引擎对应日志记录器
#        ts_folder = datetime.now().strftime("%Y%m%d_%H%M%S")  # 当前时间
#        engine_logger = get_logger(engine, ts_folder)

        # 错误信息格式
        error_msg = (
            f"[{engine}] 请求失败\n"
            f"关键词: {term}\n"
            f"请求URL: {request_url}\n"
            f"异常类型: {err_type}\n"
            f"错误信息: {error_text}\n"
            f"耗时: {elapsed:.2f}s\n"
#            f"异常信息:\n{tb_str}"fetch()
        )

        # 记录到主日志
#        main_logger.error(error_msg)

        # 记录到该引擎日志
        logger.error(error_msg)

        return {
            "engine": engine,
            "term": term,
            "status": 0,
            "elapsed": elapsed,
            "is_timeout": True,
            "success": False,
            "exception": f"{err_type}: {error_text}",
            "content_size": 0,
            "timestamp": time.time()  #时间戳
        }

# ========= 保存CSV =========
#def save_csv(engine, ts_folder, rows):
#    folder = f"csv/{engine}"
#    os.makedirs(folder, exist_ok=True)
#    file = os.path.join(folder, f"{ts_folder}.csv")
#    with open(file, "w", newline="", encoding="utf-8") as f:
#        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
#        writer.writeheader()
#        writer.writerows(rows)

# ========= 监控逻辑 =========
async def monitor():
    ts_folder = datetime.now().strftime("%Y%m%d_%H%M%S")

    status_window = {
        engine: {
            "success_rate": deque(maxlen=3),
            "timeout_rate": deque(maxlen=3),
            "exception_rate": deque(maxlen=3),
            "error_rate": deque(maxlen=3),  # 程序异常，如 status=0
            "csv_data": [],
            "logger": get_logger(engine, ts_folder)
        } for engine in SEARCH_ENGINES
    }

    for minute in range(MONITOR_DURATION_MINUTES):
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        main_logger.info(f"\n===== 第 {minute + 1} 分钟监控 =====")

        # 添加日志分隔线到每个引擎日志
        for engine in SEARCH_ENGINES:
            logger = status_window[engine]["logger"]
            logger.info("\n" + "=" * 50)
            logger.info(f"第 {minute + 1} 分钟请求开始时间：{now_str}")
            logger.info("=" * 50 + "\n")

        async with aiohttp.ClientSession() as session:
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = []

            for engine in SEARCH_ENGINES:
                for _ in range(5):
                    term = random.choice(SEARCH_TERMS)
                    async def task(engine=engine, term=term):
                        async with sem:
                            s = status_window[engine]
                            logger = s["logger"]
                            r = await fetch(session, engine, term, logger)
                            s["csv_data"].append({
                                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                **r
                            })
                            logger.info(f"[{term}] 状态: {r['status']} 成功: {r['success']} 耗时: {r['elapsed']:.2f}s 大小: {r['content_size']:.2f}KB")
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
            exceptions = sum(1 for r in group if r["status"] in (500, 502, 504))  # 仅统计5xx
            errors = sum(1 for r in group if r["status"] == 0)  # 程序错误

            sr = round(success / total * 100, 2)
            tr = round(timeout / total * 100, 2)
            er = round(exceptions / total * 100, 2)
            err = round(errors / total * 100, 2)

            s = status_window[engine]
            s["success_rate"].append(sr)
            s["timeout_rate"].append(tr)
            s["exception_rate"].append(er)
            s["error_rate"].append(err)

            # 日志汇总输出
            log = s["logger"]
            log.info(f"第 {minute + 1} 分钟汇总：")
            log.info(f"总请求数: {total} | 成功数: {success} | 超时数: {timeout} | 异常数: {exceptions} | 错误数: {errors}")
            log.info(f"成功率: {sr}% | 超时率: {tr}% | 异常率: {er}% | 错误率: {err}%")
            log.info("-" * 50 + "\n")

            # 告警逻辑
            if len(s["success_rate"]) == 3 and all(x < THRESHOLD_SUCCESS_RATE for x in s["success_rate"]):
                await send_alert(
                    f"{engine.upper()} 成功率告警",
                    f"连续3分钟成功率低于 {THRESHOLD_SUCCESS_RATE}%",
                    list(s["success_rate"]),
                    metric_label="成功率",
                    engine = engine
                )

            if len(s["timeout_rate"]) == 3 and all(x > THRESHOLD_TIMEOUT_RATE for x in s["timeout_rate"]):
                await send_alert(
                    f"{engine.upper()} 超时率告警",
                    f"连续3分钟超时率高于 {THRESHOLD_TIMEOUT_RATE}%",
                    list(s["timeout_rate"]),
                    metric_label="超时率",
                    engine = engine
                )
            group.sort(key=lambda x: x.get("timestamp", 0))  # 根据时间戳升序排序
            # 连续 3 次出现 5xx 的错误告警（在当前分钟内）
            codes = [r["status"] for r in group]
            found_3_in_row = any(
                codes[i] in (500, 502, 504) and
                codes[i + 1] in (500, 502, 504) and
                codes[i + 2] in (500, 502, 504)
                for i in range(len(codes) - 2)
            )
            if found_3_in_row:
                await send_alert(
                    f"{engine.upper()} 连续3次异常请求告警",
                    "本分钟内出现连续3次异常请求（5xx），触发预警",
                    engine = engine
                )

        await asyncio.sleep(60)

    # 保存csv
#    for engine in SEARCH_ENGINES:
#        save_csv(engine, ts_folder, status_window[engine]["csv_data"])

# ========= 启动 =========
if __name__ == "__main__":
    asyncio.run(monitor())
