import aiohttp
import asyncio
import time
import os
import logging
from urllib.parse import quote
from statistics import mean
from datetime import datetime

# ========== 配置 ==========
SEARCH_TERMS = ["Apple", "Bread", "Cheese", "Salmon", "Chocolate",
                "Spinach", "Yogurt", "Pasta", "Almond", "Eggplant"]

SEARCH_ENGINES = ["google", "bing", "yandex", "duckduckgo"]

TIMEOUT_SECONDS = 60
MIN_CONTENT_SIZE_KB = 10
CONCURRENCY = 10
RUN_DURATION_SECONDS = 150
SCHEDULE_INTERVAL = 300

SAVE_EMPTY_RESPONSE = False
ENABLE_SAVE_RESPONSE = False
ENABLE_CONSOLE_LOG = False
ENABLE_FILE_LOG = True

DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=e05a6891a68b9b3b1cfacf8dcf5852bf647457439261362fac0a1e096951bfa9"
DINGTALK_KEYWORD = "test"

# ========== 主运行日志（仅记录流程） ==========
main_logger = logging.getLogger("main")
main_logger.setLevel(logging.INFO)
if ENABLE_FILE_LOG:
    file_handler = logging.FileHandler("run.log", mode="a", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    main_logger.addHandler(file_handler)


def log_print(message):
    if ENABLE_CONSOLE_LOG:
        print(message)
    if ENABLE_FILE_LOG:
        main_logger.info(message)


# ========== 独立引擎日志 ==========
def get_engine_logger(engine: str, ts_folder: str):
    os.makedirs(f"logs/{engine}", exist_ok=True)
    logger = logging.getLogger(f"{engine}_{ts_folder}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.FileHandler(f"logs/{engine}/{ts_folder}.log", mode="w", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(handler)
    return logger


# ========== 引擎配置 ==========
ENGINE_DOMAINS = {
    "google": "www.google.com",
    "bing": "www.bing.com",
    "yandex": "yandex.com",
    "duckduckgo": "duckduckgo.com"
}

def build_service_config(engine):
    domain = ENGINE_DOMAINS[engine]
    return {
        'url': "https://scraperapi.thordata.com/request",
        'method': 'POST',
        'headers': {
            "Authorization": "Bearer 5d7caa7f1e33019f9b1851e179415bc9",
            "Content-Type": "application/json"
        },
        'json_payload': lambda q: {
            "url": f"https://{domain}/search?q={quote(q)}",
            "json": "2"
        }
    }


# ========== 单次请求 ==========
async def handle_request(session, semaphore, engine, term, req_num, ts_folder, logger):
    async with semaphore:
        config = build_service_config(engine)
        start_time = time.time()
        result = {
            'engine': engine,
            'term': term,
            'req_num': req_num,
            'success': False,
            'status_code': None,
            'error': '',
            'content_size_kb': 0,
            'elapsed_ms': 0,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            async with session.request(
                method=config['method'],
                url=config['url'],
                headers=config.get('headers', {}),
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
                json=config['json_payload'](term)
            ) as response:

                content = await response.text()
                elapsed = (time.time() - start_time) * 1000
                result.update({
                    'status_code': response.status,
                    'elapsed_ms': elapsed,
                    'content_size_kb': len(content.encode('utf-8')) / 1024
                })

                logger.info(f"[{term}] 状态码={response.status}, 耗时={elapsed:.1f}ms, 大小={result['content_size_kb']:.2f}KB")

                if "text/html" in response.headers.get("Content-Type", ""):
                    result['error'] = "返回了 HTML 页面"
                    logger.info(f"❗ {term} 返回 HTML，耗时={elapsed:.1f}ms")
                    return result

                if response.status != 200:
                    result['error'] = f"HTTP错误: {response.status}"
                    logger.info(f"❗ {term} HTTP错误: {response.status}，耗时={elapsed:.1f}ms")
                    return result

                if not content:
                    result['error'] = "空响应"
                    logger.info(f"❗ {term} 空响应，耗时={elapsed:.1f}ms")
                    return result

                if result['content_size_kb'] < MIN_CONTENT_SIZE_KB:
                    result['error'] = f"内容过小 ({result['content_size_kb']:.2f}KB)"
                    logger.info(f"❗ {term} 内容过小: {result['content_size_kb']:.2f}KB，耗时={elapsed:.1f}ms")
                    return result

                if ENABLE_SAVE_RESPONSE:
                    folder = f"data/{engine}/{ts_folder}"
                    os.makedirs(folder, exist_ok=True)
                    filename = f"{term[:20].replace('/', '_')}_{req_num}_{int(time.time())}.txt"
                    with open(os.path.join(folder, filename), 'w', encoding='utf-8') as f:
                        f.write(content)

                result['success'] = True
                return result

        except asyncio.TimeoutError:
            elapsed = (time.time() - start_time) * 1000
            result['error'] = "请求超时"
            result['elapsed_ms'] = elapsed
            logger.info(f"❗ {term} 请求超时，耗时={elapsed:.1f}ms")
        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            result['error'] = f"异常: {str(e)}"
            result['elapsed_ms'] = elapsed
            logger.info(f"❗ {term} 异常: {str(e)}，耗时={elapsed:.1f}ms")

        return result


# ========== 数据统计 ==========
def summarize_stats(engine, results):
    total = len(results)
    success = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    return {
        "engine": engine,
        "concurrency": CONCURRENCY,
        "total_requests": total,
        "success_count": len(success),
        "fail_count": len(failed),
        "success_rate": round(len(success) / total * 100, 2) if total else 0.0,
        "avg_elapsed_ms": round(mean(r['elapsed_ms'] for r in success), 2) if success else 0.0,
        "avg_size_kb": round(mean(r['content_size_kb'] for r in success), 2) if success else 0.0,
    }


# ========== 钉钉通知 ==========
async def send_dingtalk_report(summary_list):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"### SERP {DINGTALK_KEYWORD} 结果汇总 - {now}"]
    for s in summary_list:
        lines.append(
            f"\n#### {s['engine'].upper()}\uff1a\n"
            f"- 并发数：{s['concurrency']}\n"
            f"- 请求数：{s['total_requests']}\n"
            f"- 成功数：{s['success_count']}\n"
            f"- 失败数：{s['fail_count']}\n"
            f"- 成功率：{s['success_rate']}%\n"
            f"- 平均耗时：{s['avg_elapsed_ms']} ms\n"
            f"- 平均大小：{s['avg_size_kb']} KB\n"
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
            if resp.status != 200:
                log_print(f"❌ 钉钉通知失败: {resp.status}")
            else:
                log_print("📢 钉钉通知已发送")


# ========== 单轮执行 ==========
async def run_test_cycle():
    results_by_engine = []
    per_engine_duration = RUN_DURATION_SECONDS / len(SEARCH_ENGINES)
    ts_folder = datetime.now().strftime("%Y%m%d_%H%M%S")

    async with aiohttp.ClientSession() as session:
        for engine in SEARCH_ENGINES:
            log_print(f"\n▶ 开始引擎: {engine.upper()} (约 {per_engine_duration:.1f}s)")
            end_time = time.time() + per_engine_duration
            semaphore = asyncio.Semaphore(CONCURRENCY)
            logger = get_engine_logger(engine, ts_folder)
            results = []
            req_num = 0

            while time.time() < end_time:
                tasks = []
                for term in SEARCH_TERMS:
                    req_num += 1
                    tasks.append(handle_request(session, semaphore, engine, term, req_num, ts_folder, logger))
                batch_results = await asyncio.gather(*tasks)
                results.extend(batch_results)

            summary = summarize_stats(engine, results)
            results_by_engine.append(summary)

    return results_by_engine


# ========== 周期执行 ==========
async def scheduled_loop():
    while True:
        log_print(f"\n⏱️ 开始测试轮次: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        summary_list = await run_test_cycle()
        for s in summary_list:
            log_print(f"📊 {s['engine'].upper()} 结果: {s}")
        await send_dingtalk_report(summary_list)
        log_print(f"\n✅ 当前轮完成，等待 {SCHEDULE_INTERVAL} 秒...\n")
        await asyncio.sleep(SCHEDULE_INTERVAL)


# ========== 启动 ==========
if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(scheduled_loop())
