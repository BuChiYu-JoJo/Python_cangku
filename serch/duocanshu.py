import aiohttp
import asyncio
import csv
import os
import time
import logging
import random
import traceback
import uuid
from urllib.parse import quote, urlencode
from datetime import datetime

# ========= 配置 =========
SEARCH_ENGINES = {
    "yandex": {
        "enabled": True,
        "domain": ["yandex.com", "yandex.ru", "yandex.uz", "yandex.kz", "yandex.com.tr"],
        "param": "text",
        "extra_params": ["lang", "lr", "rstr", "p", "within"]
    },
    "duckduckgo": {
        "enabled": False,
        "domain": "duckduckgo.com",
        "param": "q",
        "extra_params": ["kl", "start", "df", "kp"]
    },
    "google": {
        "enabled": False,
        "domain": ["www.google.com", "www.google.ad", "www.google.ca", "www.google.co.jp", "www.google.com.vn"],
        "param": "q",
        "extra_params": ["gl", "hl", "cr", "lr", "location", "uule", "tbm", "start", "num", "safe"]
    },
    "bing": {
        "enabled": False,
        "domain": "www.bing.com",
        "param": "q",
        "extra_params": ["cc", "mkt", "location", "lat", "lon", "first", "count", "adlt"]
    },
}

SEARCH_TERMS = [
    "Apple", "Bread", "Cheese", "Salmon", "Chocolate",
    "Spinach", "Yogurt", "Pasta", "Almond", "Eggplant"
]

AUTH_HEADER = {
    "Authorization": "Bearer 5d7caa7f1e33019f9b1851e179415bc9",
    "Content-Type": "application/json"
}

CONCURRENCY = 1
TIMEOUT_SECONDS = 60
MIN_CONTENT_SIZE_KB = 0
ENABLE_SAVE_RESPONSE = True
REQUEST_COUNT_PER_ENGINE = 1000

# ========= 日志配置 =========
def get_logger(engine_name):
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(engine_name)
    if not logger.handlers:
        handler = logging.FileHandler(f"logs/{engine_name}.log", encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

# ========= 构建查询参数 =========
def random_param_value(param, engine=None):
    if param == "hl":
        return random.choice(["en", "fr", "ru", "ja", "cn"])
    elif param == "lang":
        return random.choice(["en", "ru", "fr", "de", "ua"])
    elif param == "lr":
        if engine == "google":
            return random.choice(["lang_en", "lang_ja", "lang_ca", "lang_ru", "lang_zh-CN"])
        elif engine == "yandex":
            return random.choice(["United+States", "Japan", "Germany", "France", "China"])
        else:
            return None
    elif param == "mkt":
        return random.choice(["en-us", "ja-jp", "fr-ca", "ru-ru", "zh-cn"])
    elif param == "location":
        # 拆分出 location 和 uule 两个参数
        loc_uule_pairs = [
            ("India", "w+CAIQICIFSW5kaWE"),
            ("United States", "w+CAIQICINVW5pdGVkK1N0YXRlcw"),
            ("Japan", "w+CAIQICIFSmFwYW4"),
            ("Brazil", "w+CAIQICIGQnJhemls"),
            ("France", "w+CAIQICIGRnJhbmNl")
        ]
        loc, uule = random.choice(loc_uule_pairs)
        return {"location": loc, "uule": uule}
    elif param == "cr":
        return random.choice(["countryAF", "countryCA", "countryCN", "countryJP", "countryUS"])
    elif param == "gl":
        return random.choice(["us", "uk", "ru", "jp", "cn"])
    elif param == "rstr":
        return random.choice(["true", None])
    elif param == "within":
        return random.choice(["0", "77", "1", "2"])
    elif param == "kl":
        return random.choice(["India", "United+States", "Japan", "France", "China"])
    elif param == "df":
        return random.choice([None, "d", "w", "m", "y"])
    elif param == "kp":
        return random.choice(["1", "-1", "-2"])
    elif param == "tbm":
        return random.choice([None, "isch", "shop", "nws", "vid"])
    elif param == "start":
        return str(random.randint(0, 20))
    elif param == "num":
        return str(random.choice([5, 10, 15]))
    elif param == "first":
        return str(random.randint(0, 20))
    elif param == "count":
        return str(random.randint(0, 50))
    elif param == "adlt":
        return random.choice(["strict", "off", None])
    elif param == "safe":
        return random.choice(["active", "off",None])
    elif param in {"lat", "lon"}:
        return str(round(random.uniform(-90, 90), 4))
    else:
        return str(random.randint(0, 10))

# ========= 构建请求 =========
def build_payload(engine, term):
    config = SEARCH_ENGINES[engine]
    domain = random.choice(config["domain"]) if isinstance(config["domain"], list) else config["domain"]
    base_url = f"https://{domain}/search"

    query = {
        config['param']: term,
        "json": "1"
    }

    used_keys = set()  # 跟踪已添加的参数
    for param in config.get("extra_params", []):
        if param in used_keys:
            continue
        value = random_param_value(param, engine)
        if value is None:
            continue
        if isinstance(value, dict):
            for k, v in value.items():
                if k not in used_keys:
                    query[k] = v
                    used_keys.add(k)
        else:
            if param not in used_keys:
                query[param] = value
                used_keys.add(param)

    query_str = urlencode(query)
    full_url = f"{base_url}?{query_str}"
    return {
        "url": full_url,
        "full_url": full_url
    }

# ========= 请求函数 =========
async def fetch(session, engine, term):
    start = time.time()
    payload = build_payload(engine, term)
    full_url = payload["full_url"]
    logger = get_logger(engine)

    base_result = {
        "engine": engine,
        "term": term,
        "status": 0,
        "elapsed": 0,
        "success": False,
        "content_size_kb": 0,
        "full_url": full_url,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "exception": "",
        "code": None,
        "response_saved": False
    }

    try:
        async with session.post(
            url="https://scraperapi.thordata.com/request",
            headers=AUTH_HEADER,
            json={"url": full_url},
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)
        ) as response:
            content = await response.text()
            elapsed = time.time() - start
            content_size = len(content.encode("utf-8")) / 1024
            success = False
            code = None

            try:
                json_data = await response.json()
                code = json_data.get("code")
                data = json_data.get("data", {})
                success = (
                    response.status == 200 and
                    code == 200 and
                    isinstance(data, dict) and
                    "task_id" in data
                )
                if not success:
                    if isinstance(data, dict):
                        base_result["exception"] = (
                                data.get("message") or
                                data.get("error") or
                                data.get("msg") or
                                str(data)
                        )
                    elif isinstance(data, str):
                        base_result["exception"] = data
                    else:
                        base_result["exception"] = str(data)

            except Exception as parse_err:
                base_result["exception"] = f"JSON解析失败: {type(parse_err).__name__}: {str(parse_err)}"

            # 保存响应内容（无论成功与否）
            if ENABLE_SAVE_RESPONSE:
                save_dir = f"responses/{engine}"
                os.makedirs(save_dir, exist_ok=True)
                status_str = "success" if success else "fail"
                safe_term = quote(term, safe='')
                filename = f"{save_dir}/{safe_term}_{status_str}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}.json"

                try:
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(content if content else "（响应为空）")
                    base_result["response_saved"] = True
                except Exception as file_err:
                    logger.error(f"[{engine}] 文件保存失败: {filename}\n错误: {file_err}")

            base_result.update({
                "status": response.status,
                "elapsed": round(elapsed, 2),
                "success": success,
                "content_size_kb": round(content_size, 2),
                "code": code
            })

            if success:
                logger.info(
                    f"{term} 状态: {response.status} | 成功 | 耗时: {elapsed:.2f}s | 大小: {content_size:.2f}KB | 已保存: {base_result['response_saved']}\nURL: {full_url}"
                )
            else:
                logger.error(
                    f"{term} 状态: {response.status} | 业务失败 | 耗时: {elapsed:.2f}s | 大小: {content_size:.2f}KB"
                    + f" | 错误: {base_result['exception']}" if base_result["exception"] else ""
                    + f" | 已保存: {base_result['response_saved']}\nURL: {full_url}"
                )

            return base_result

    except Exception as e:
        elapsed = time.time() - start
        tb_str = traceback.format_exc()
        error_type = type(e).__name__
        error_message = str(e)
        exception_text = f"{error_type}: {error_message}" if error_message else error_type
        base_result.update({
            "elapsed": round(elapsed, 2),
            "exception": exception_text
        })

        # 异常情况下尝试保存错误详情
        if ENABLE_SAVE_RESPONSE:
            save_dir = f"responses/{engine}"
            os.makedirs(save_dir, exist_ok=True)
            safe_term = quote(term, safe='')
            filename = f"{save_dir}/{safe_term}_exception_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}.txt"
            try:
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(f"请求异常：{exception_text}\n")
                    f.write(f"URL: {full_url}\n\n堆栈信息:\n{tb_str}")
                base_result["response_saved"] = True
            except Exception as file_err:
                logger.error(f"[{engine}] 异常文件保存失败: {filename}\n错误: {file_err}")

        logger.error(
            f"[{engine}] 请求失败: {term} | 错误: {exception_text} | 已保存: {base_result['response_saved']}\nURL: {full_url}\n堆栈:\n{tb_str}"
        )
        return base_result


# ========= 每个引擎轮询请求 =========
async def run_for_engine(engine, term_list):
    sem = asyncio.Semaphore(CONCURRENCY)
    results = []
    engine_logger = get_logger(engine)

    async with aiohttp.ClientSession() as session:
        async def task(term):
            async with sem:
                result = await fetch(session, engine, term)
                results.append(result)

        tasks = [asyncio.create_task(task(term)) for term in term_list]
        await asyncio.gather(*tasks)

    return results

# ========= 主调度器 =========
async def run_all():
    for engine in SEARCH_ENGINES:
        config = SEARCH_ENGINES[engine]
        engine_logger = get_logger(engine)

        if not config.get("enabled", False):
            engine_logger.info(f"⏩ 引擎 {engine} 被禁用，跳过")
            continue

        engine_logger.info(f"🚀 开始执行引擎：{engine}")
        terms = random.choices(SEARCH_TERMS, k=REQUEST_COUNT_PER_ENGINE)

        start_time = time.time()
        results = await run_for_engine(engine, terms)
        end_time = time.time()
        elapsed_total = round(end_time - start_time, 2)

        save_single_engine_to_csv(engine, results, elapsed_total)

# ========= 保存 CSV =========
def save_single_engine_to_csv(engine, data, total_elapsed):
    os.makedirs("csv", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    fieldnames = [
        "engine", "term", "status", "code", "success", "content_size_kb",
        "elapsed", "full_url", "exception"
    ]
    header_mapping = {
        "engine": "引擎",
        "term": "关键词",
        "status": "状态码",
        "code": "业务状态码",
        "success": "是否成功",
        "content_size_kb": "响应内容大小(KB)",
        "elapsed": "响应时间(s)",
        "full_url": "完整请求URL",
        "exception": "错误备注"
    }

    if not data:
        print(f"⚠️ 引擎 {engine} 无数据可保存")
        return

    # 计算统计数据
    total = len(data)
    success_data = [r for r in data if r.get("success")]
    success_count = len(success_data)
    success_rate = f"{(success_count / total * 100):.2f}%" if total else "0.00%"
    avg_size = round(sum(r.get("content_size_kb", 0) for r in success_data) / success_count, 2) if success_count else 0
    avg_elapsed = round(sum(r.get("elapsed", 0) for r in success_data) / success_count, 2) if success_count else 0

    # 准备平均行（字段值用空字符串填充不相关列）
    average_row = {
        "engine": "平均",
        "term": "",
        "status": "",
        "code": "",
        "success": success_rate,
        "content_size_kb": avg_size,
        "elapsed": avg_elapsed,
        "full_url": "",
        "exception": ""
    }
   #总计行
    total_row = {
        "engine": "总计",
        "term": "",
        "status": "",
        "code": "",
        "success": f"{success_count}/{total}",
        "content_size_kb": "",
        "elapsed": total_elapsed,
        "full_url": "",
        "exception": ""
    }

    filename = f"csv/{engine}_results_{timestamp}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow({k: header_mapping[k] for k in fieldnames})  # 表头
        writer.writerow({k: average_row.get(k, "") for k in fieldnames})  # 平均
        writer.writerow({k: total_row.get(k, "") for k in fieldnames})  # 总计
        for row in data:
            writer.writerow({k: row.get(k, "") for k in fieldnames})  # 数据

    print(f"✅ 引擎 {engine} 共 {len(data)} 条记录保存至 {filename}")

# ========= 启动入口 =========
if __name__ == "__main__":
    asyncio.run(run_all())
