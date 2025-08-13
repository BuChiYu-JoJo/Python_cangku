import asyncio
import aiohttp
import csv
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional

# ===================== 配置区（按需修改） =====================

# 目标接口
TARGET_URL = "http://ipinfo.io/json"

# 代理网关配置（基座，不含 -continent-xx）
PROXY_HOST = "nqpr29uk.thordata.online"
PROXY_PORT = 9999
PROXY_USER_BASE = "td-customer-zxs1261977221"
PROXY_PASS = "k1j7696pu"

# 大洲与请求次数配置（可自由增删&修改次数）
# 大洲代码：af=非洲, as=亚洲, eu=欧洲, na=北美, sa=南美, oc=大洋洲
CONTINENT_REQUESTS: Dict[str, int] = {
    "as": 100,
    "eu": 100,
    "na": 100,
    "sa": 100,
    "oc": 100,
    "af": 100,
}

# 速率与并发
REQUESTS_PER_SECOND = 10
CONCURRENCY = 20
TIMEOUT_SECONDS = 30

# CSV 输出文件（UTF-8 编码）
CSV_FILE = f"continent_ipinfo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# 是否在控制台打印每次结果
ENABLE_CONSOLE_LOG = True

# ============================================================

# 常见国家码 -> 大洲 映射
COUNTRY_TO_CONTINENT: Dict[str, str] = {
    # Africa
    "DZ":"af","AO":"af","BJ":"af","BW":"af","BF":"af","BI":"af","CM":"af","CV":"af","CF":"af","TD":"af","KM":"af",
    "CD":"af","CG":"af","CI":"af","DJ":"af","EG":"af","GQ":"af","ER":"af","ET":"af","GA":"af","GM":"af","GH":"af",
    "GN":"af","GW":"af","KE":"af","LS":"af","LR":"af","LY":"af","MG":"af","MW":"af","ML":"af","MR":"af","MU":"af",
    "YT":"af","MA":"af","MZ":"af","NA":"af","NE":"af","NG":"af","RE":"af","RW":"af","ST":"af","SN":"af","SC":"af",
    "SL":"af","SO":"af","ZA":"af","SS":"af","SD":"af","SZ":"af","TZ":"af","TG":"af","TN":"af","UG":"af","EH":"af",
    "ZM":"af","ZW":"af","SH":"af",

    # Asia
    "AF":"as","AM":"as","AZ":"as","BH":"as","BD":"as","BT":"as","BN":"as","KH":"as","CN":"as","GE":"as","HK":"as",
    "IN":"as","ID":"as","IR":"as","IQ":"as","IL":"as","JP":"as","JO":"as","KZ":"as","KW":"as","KG":"as","LA":"as",
    "LB":"as","MO":"as","MY":"as","MV":"as","MN":"as","MM":"as","NP":"as","KP":"as","OM":"as","PK":"as","PH":"as",
    "QA":"as","SA":"as","SG":"as","KR":"as","LK":"as","PS":"as","SY":"as","TW":"as","TJ":"as","TH":"as","TR":"as",
    "TM":"as","AE":"as","UZ":"as","VN":"as","YE":"as",

    # Europe
    "AL":"eu","AD":"eu","AT":"eu","BY":"eu","BE":"eu","BA":"eu","BG":"eu","HR":"eu","CY":"eu","CZ":"eu","DK":"eu",
    "EE":"eu","FO":"eu","FI":"eu","FR":"eu","DE":"eu","GI":"eu","GR":"eu","GG":"eu","HU":"eu","IS":"eu","IE":"eu",
    "IM":"eu","IT":"eu","JE":"eu","XK":"eu","LV":"eu","LI":"eu","LT":"eu","LU":"eu","MT":"eu","MD":"eu","MC":"eu",
    "ME":"eu","NL":"eu","MK":"eu","NO":"eu","PL":"eu","PT":"eu","RO":"eu","RU":"eu","SM":"eu","RS":"eu","SK":"eu",
    "SI":"eu","ES":"eu","SE":"eu","CH":"eu","UA":"eu","GB":"eu","VA":"eu",

    # North America
    "AI":"na","AG":"na","AW":"na","BS":"na","BB":"na","BZ":"na","BM":"na","BQ":"na","VG":"na","CA":"na","KY":"na",
    "CR":"na","CU":"na","CW":"na","DM":"na","DO":"na","SV":"na","GL":"na","GD":"na","GP":"na","GT":"na","HT":"na",
    "HN":"na","JM":"na","MQ":"na","MX":"na","MS":"na","NI":"na","PA":"na","PR":"na","BL":"na","KN":"na","LC":"na",
    "MF":"na","SX":"na","PM":"na","VC":"na","TT":"na","TC":"na","US":"na","VI":"na","UM":"na",

    # South America
    "AR":"sa","BO":"sa","BR":"sa","CL":"sa","CO":"sa","EC":"sa","FK":"sa","GF":"sa","GY":"sa","PY":"sa","PE":"sa",
    "SR":"sa","UY":"sa","VE":"sa",

    # Oceania
    "AS":"oc","AU":"oc","CK":"oc","FJ":"oc","PF":"oc","GU":"oc","KI":"oc","MH":"oc","FM":"oc","NR":"oc","NC":"oc",
    "NZ":"oc","NU":"oc","MP":"oc","PW":"oc","PG":"oc","WS":"oc","SB":"oc","TK":"oc","TO":"oc","TV":"oc","VU":"oc",
    "WF":"oc"
}

# 中国标准时间（UTC+8）时区对象
CST_TZ = timezone(timedelta(hours=8))

def country_to_continent(country_code: str) -> Optional[str]:
    if not country_code:
        return None
    return COUNTRY_TO_CONTINENT.get(country_code.upper())

def build_proxy_url(continent: str) -> str:
    """
    代理用户名追加 -continent-xx；aiohttp 推荐 http 方案的代理 URL。
    """
    user = f"{PROXY_USER_BASE}-continent-{continent}"
    return f"http://{user}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

def now_cst_str() -> str:
    """
    返回中国标准时间（UTC+8）的时间字符串，UTF-8 写入，格式示例：2025-08-13 21:05:12.345+0800
    """
    dt = datetime.now(CST_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")

async def fetch_once(session: aiohttp.ClientSession, sem: asyncio.Semaphore, req_id: int, continent: str) -> Tuple:
    """
    不使用分段超时；对所有 >=400 的状态码统一按 HTTPError 处理且不读正文；
    CSV 的“响应耗时(秒)”仅记录网络阶段（不含排队）。
    """
    proxy_url = build_proxy_url(continent)
    request_time_cst = now_cst_str()

    enqueue_ts = time.perf_counter()   # 任务入队时间
    start_ts = None                    # 网络阶段起点

    status = None
    error_type = ""
    error_msg = ""
    resp_size_bytes = 0
    ip = ""
    country = ""
    success = False

    timeout = aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)

    try:
        async with sem:  # 这里可能会等待排队，但不计入 network_time
            start_ts = time.perf_counter()
            async with session.get(
                TARGET_URL,
                proxy=proxy_url,
                timeout=timeout,
                headers={"Accept": "application/json"}
            ) as resp:
                status = resp.status

                # -------- 通用化错误处理：>=400 一律按 HTTPError 记录，不读正文 --------
                if status >= 400:
                    success = False
                    # 直接使用服务器给的 reason（如 "Too Many Requests"）
                    reason = getattr(resp, "reason", "") or ""
                    error_type = "HTTPError"
                    error_msg = f"{status} {reason}".strip()
                    # 不读取 body，避免把读正文阶段的超时混为 TimeoutError
                    await resp.release()
                    network_time_s = time.perf_counter() - start_ts

                else:
                    # 2xx 正常读取（我们需要 body 来解析 ip/country）
                    content = await resp.read()
                    resp_size_bytes = len(content)
                    if status == 200:
                        try:
                            data = json.loads(content.decode("utf-8", errors="ignore"))
                            ip = str(data.get("ip", "")).strip()
                            country = str(data.get("country", "")).strip()
                            success = True
                            error_type = ""
                            error_msg = ""
                        except Exception as je:
                            success = False
                            error_type = type(je).__name__
                            error_msg = f"JSONDecodeError: {je}"
                    else:
                        # 少见分支（例如关闭自动重定向时可能出现 3xx）
                        success = False
                        reason = getattr(resp, "reason", "") or ""
                        error_type = "HTTPError"
                        error_msg = f"{status} {reason}".strip()

                    network_time_s = time.perf_counter() - start_ts

    except Exception as e:
        # 在进入 resp 前或网络阶段中发生的真实异常（含 TimeoutError）
        network_time_s = time.perf_counter() - start_ts if start_ts is not None else 0.0
        error_type = type(e).__name__
        error_msg = str(e)

    # 仅网络阶段耗时（不含排队）
    queue_wait_s = max(0.0, (start_ts - enqueue_ts)) if start_ts is not None else 0.0
    network_time_s = round(network_time_s, 3)
    queue_wait_s = round(queue_wait_s, 3)

    # 大洲匹配判定
    requested_continent = continent.lower()
    returned_continent = country_to_continent(country) if country else None
    if country:
        if returned_continent is None:
            match = "未知"
        else:
            match = "true" if returned_continent == requested_continent else "false"
    else:
        match = "n/a"

    resp_size_kb = round(resp_size_bytes / 1024, 2)

    if ENABLE_CONSOLE_LOG:
        print(
            f"[{req_id}] {request_time_cst} 大洲={continent} 状态={status} IP={ip or '-'} 国家={country or '-'} "
            f"匹配={match} 排队={queue_wait_s}s 网络={network_time_s}s 大小={resp_size_kb}KB "
            f"错误={error_type or '-'} {error_msg or ''}",
            flush=True
        )

    # CSV：响应耗时(秒) 只写网络阶段；错误与状态码一致（>=400 必为 HTTPError）
    return [
        req_id,                            # 请求编号
        request_time_cst,                  # 请求时间（中国标准时间）
        TARGET_URL,                        # 请求URL
        continent,                         # 请求大洲
        status if status is not None else "-",  # HTTP状态码
        success,                           # 是否成功
        network_time_s,                    # 响应耗时(秒) = 仅网络
        resp_size_kb,                      # 响应大小(KB)
        ip,                                # 返回IP
        country,                           # 返回国家
        returned_continent or "",          # 返回国家所属大洲
        match,                             # 是否匹配大洲
        error_type,                        # 错误类型
        error_msg                          # 错误信息
    ]

def make_workload(continent_requests: Dict[str, int]) -> list:
    """
    将 {continent: count} 展平为任务序列（轮询排布）。
    """
    seq = []
    remaining = continent_requests.copy()
    while any(v > 0 for v in remaining.values()):
        for cont in list(remaining.keys()):
            if remaining[cont] > 0:
                seq.append(cont)
                remaining[cont] -= 1
    return seq

async def main():
    # 初始化 CSV（UTF-8 编码，中文表头）
    with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "请求编号",
            "请求时间",
            "请求URL",
            "请求大洲",
            "HTTP状态码",
            "是否成功",
            "响应耗时(秒)",
            "响应大小(KB)",
            "返回IP",
            "返回国家",
            "返回国家所属大洲",
            "是否匹配大洲",
            "错误类型",
            "错误信息",
        ])

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    # 构造任务序列
    workload = make_workload(CONTINENT_REQUESTS)

    # 速率控制节拍
    interval = 1.0 / max(1, REQUESTS_PER_SECOND)

    req_id = 0
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for continent in workload:
            req_id += 1
            tasks.append(fetch_once(session, sem, req_id, continent))
            await asyncio.sleep(interval)

        # 边完成边写入，避免占用过多内存
        pending = set(asyncio.create_task(t) for t in tasks)

        try:
            with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        row = task.result()
                        writer.writerow(row)
        except KeyboardInterrupt:
            print("检测到 Ctrl+C，中断写入。")

    print(f"完成。结果已保存到：{CSV_FILE}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("脚本退出。")
