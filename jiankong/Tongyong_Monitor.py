import aiohttp
import asyncio
import time
import os
import logging
from datetime import datetime
from collections import deque
from statistics import mean

# ========== 配置 ==========
#URL_LIST = [
#    "https://www.google.com",
#    "https://www.bing.com",
#    "https://www.wikipedia.org"
]


URL_LIST = [
    "https://httpbin.org/status/504",  # 5xx 错误
#    "http://10.255.255.1",              # 超时
#    "http://nonexistent.openai.invalid"  # 异常
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

CONCURRENCY = 5
TIMEOUT_SECONDS = 10
MIN_CONTENT_SIZE_KB = 10
THRESHOLD_SUCCESS_RATE = 80.0
THRESHOLD_TIMEOUT_RATE = 10.0

ENABLE_SAVE_RESPONSE = False
ENABLE_CONSOLE_LOG = True
ENABLE_FILE_LOG = True

DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=e05a6891a68b9b3b1cfacf8dcf5852bf647457439261362fac0a1e096951bfa9"
DINGTALK_KEYWORD = "Universal_Scraping_test"

# ========== 日志 ==========
class DailyRotatingFileHandler(logging.Handler):
    def __init__(self, log_dir="logs", base_filename="run", encoding="utf-8"):
        super().__init__()
        self.log_dir = log_dir
        self.base_filename = base_filename
        self.encoding = encoding
        os.makedirs(log_dir, exist_ok=True)
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.stream = self._open_stream()

    def _get_log_path(self):
        return os.path.join(self.log_dir, f"{self.base_filename}_{self.current_date}.log")

    def _open_stream(self):
        return open(self._get_log_path(), mode="a", encoding=self.encoding)

    def emit(self, record):
        try:
            now = datetime.now().strftime("%Y-%m-%d")
            if now != self.current_date:
                self.current_date = now
                self.stream.close()
                self.stream = self._open_stream()

            msg = self.format(record)
            self.stream.write(msg + "\n")
            self.stream.flush()
        except Exception:
            self.handleError(record)

    def close(self):
        if self.stream:
            try:
                self.stream.close()
            except Exception:
                pass
        super().close()

# 设置 logger
logger = logging.getLogger("monitor")
logger.setLevel(logging.INFO)

if ENABLE_FILE_LOG:
    handler = DailyRotatingFileHandler(log_dir="logs", base_filename="run")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(handler)

def log_print(msg):
    if ENABLE_CONSOLE_LOG:
        print(msg)
    if ENABLE_FILE_LOG:
        logger.info(msg)

# ========== 钉钉推送 ==========
async def send_alert(title, content, data_list=None, metric_label="指标值", url=None):
    data_section = ""
    if data_list:
        data_section = f"<br><br>近3分钟{metric_label}：<br>{', '.join(str(x) + '%' for x in data_list)}"

    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"{DINGTALK_KEYWORD} | {title}" + (f" | {url}_{datetime.now().strftime('%H:%M:%S')}" if url else ""),
            "text": f"### {DINGTALK_KEYWORD} | {title}<br><br>{content}{data_section}"
        }
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(DINGTALK_WEBHOOK, json=payload) as resp:
                log_print(f"📢 钉钉通知状态: {resp.status}")
    except Exception as e:
        log_print(f"❌ 钉钉发送异常: {e}")

# ========== 单次请求 ==========
async def fetch(session, url):
    payload = SERVICE_CONFIG["json_payload"](url)
    headers = SERVICE_CONFIG["headers"]
    endpoint = SERVICE_CONFIG["endpoint"]

    start_time = time.time()
    result = {
        "url": url,
        "status": None,
        "elapsed": 0,
        "is_timeout": False,
        "success": False,
        "content_size": 0,
        "timestamp": time.time()
    }

    try:
#        async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as resp:  #调试用
        async with session.post(endpoint, json=payload, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as resp:
            text = await resp.text()
            elapsed = time.time() - start_time
            content_size = len(text.encode("utf-8")) / 1024
            status = resp.status

            result.update({
                "status": status,
                "elapsed": elapsed,
                "content_size": content_size,
                "success": (status == 200 and content_size >= MIN_CONTENT_SIZE_KB),
            })

            if status >= 500:
                log_print(f"⚠️ 5xx 响应: URL={url}, 状态={status}, 大小={content_size:.2f}KB, 耗时={elapsed:.2f}s")
            else:
                log_print(f"✅ 请求成功: URL={url}, 状态={status}, 大小={content_size:.2f}KB, 耗时={elapsed:.2f}s, 成功判断={result['success']}")
            return result

    except Exception as e:
        elapsed = time.time() - start_time
        error_type = type(e).__name__
        error_str = str(e) or repr(e)

        result.update({
            "status": 0,
            "elapsed": elapsed,
            "is_timeout": isinstance(e, (asyncio.TimeoutError, aiohttp.ClientTimeout)),
            "success": False
        })

        log_print(f"❌ 请求异常: URL={url}, 错误类型={error_type}, 错误内容={error_str}")
        return result


# ========== 主监控函数 ==========
async def monitor_loop():
    status_window = {
        url: {
            "success_rate": deque(maxlen=3),
            "timeout_rate": deque(maxlen=3),
            "logger": logger,
        } for url in URL_LIST
    }

    while True:
        current_minute = datetime.now().strftime("%Y-%m-%d %H:%M")
        log_print(f"\n===== ⏱️ {current_minute} 监控开始 =====")

        results = []
        async with aiohttp.ClientSession() as session:
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = []
            for url in URL_LIST:
                for _ in range(5):
                    async def task(u=url):
                        async with sem:
                            r = await fetch(session, u)
                            results.append(r)
                    tasks.append(task())
            await asyncio.gather(*tasks)

        # 汇总并打印每个 URL 的统计
        from collections import defaultdict
        grouped = defaultdict(list)
        for r in results:
            grouped[r["url"]].append(r)

        for url, group in grouped.items():
            group.sort(key=lambda x: x.get("timestamp", 0))
            total = len(group)
            success = sum(1 for r in group if r["success"])
            timeout = sum(1 for r in group if r["is_timeout"])
            exceptions = sum(1 for r in group if r["status"] in (500, 502, 504))

            sr = round(success / total * 100, 2)
            tr = round(timeout / total * 100, 2)
            er = round(exceptions / total * 100, 2)

            # 每分钟统计打印
            log_print(f"📊 {url}：成功 {success}/{total}，超时 {timeout}，异常 {exceptions}，成功率={sr}%，超时率={tr}%，异常率={er}%")

            s = status_window[url]
            s["success_rate"].append(sr)
            s["timeout_rate"].append(tr)

            if len(s["success_rate"]) == 3 and all(x < THRESHOLD_SUCCESS_RATE for x in s["success_rate"]):
                await send_alert(
                    f"{url} 成功率告警",
                    f"连续3分钟成功率低于 {THRESHOLD_SUCCESS_RATE}%",
                    list(s["success_rate"]),
                    "成功率",
                    url
                )

            if len(s["timeout_rate"]) == 3 and all(x > THRESHOLD_TIMEOUT_RATE for x in s["timeout_rate"]):
                await send_alert(
                    f"{url} 超时率告警",
                    f"连续3分钟超时率高于 {THRESHOLD_TIMEOUT_RATE}%",
                    list(s["timeout_rate"]),
                    "超时率",
                    url
                )

            # 连续 3 次 5xx 状态码
            codes = [r["status"] for r in group]
            found_3_5xx = any(
                codes[i] in (500, 502, 504) and
                codes[i + 1] in (500, 502, 504) and
                codes[i + 2] in (500, 502, 504)
                for i in range(len(codes) - 2)
            )
            if found_3_5xx:
                await send_alert(
                    f"{url} 连续3次异常请求",
                    f"{url} 出现连续3次 5xx 状态码异常请求",
                    url=url
                )

        log_print(f"===== ✅ {current_minute} 监控结束 =====\n")
        await asyncio.sleep(60)

# ========== 启动 ==========
if __name__ == "__main__":
    asyncio.run(monitor_loop())
