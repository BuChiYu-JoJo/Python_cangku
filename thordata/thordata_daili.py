import requests
from requests.auth import HTTPProxyAuth
import time
import json
import hmac
import hashlib
import base64
import urllib.parse

# =============== 钉钉推送功能 ================
def send_dingtalk_report(text, webhook, keyword="代理test"):
    headers = {'Content-Type': 'application/json'}
    message = {
        "msgtype": "text",
        "text": {
            "content": f"{keyword}\n\n{text}"
        }
    }
    try:
        response = requests.post(webhook, data=json.dumps(message), headers=headers, timeout=10)
        if response.status_code == 200:
            print("✅ 已发送钉钉通知")
        else:
            print(f"❌ 钉钉发送失败: {response.status_code} {response.text}")
    except Exception as e:
        print(f"❌ 钉钉发送异常: {e}")

# =============== 住宅代理测试 ================
def test_residential_proxy():
    proxy = {
        'http': 'http://rmmsg2sa.pr.thordata.net:9999',
        'https': 'http://rmmsg2sa.pr.thordata.net:9999',
    }
    auth = HTTPProxyAuth('td-customer-GH43726', 'GH43726')
    url = 'http://ipinfo.io'  # 必须用 HTTP 避免 CONNECT 隧道 407 错误
    success_count = 0
    result_lines = []

    print("=== 住宅代理测试开始 ===")
    for i in range(1, 11):
        try:
            start = time.time()
            response = requests.get(url, proxies=proxy, auth=auth, timeout=10)
            elapsed = round(time.time() - start, 2)
            response.raise_for_status()
            data = response.json()
            success_count += 1
            msg = f"[{i}] ✅ 成功 - IP: {data.get('ip')}, 地区: {data.get('city')}, 国家: {data.get('country')}, 延迟: {elapsed}s"
        except Exception as e:
            elapsed = round(time.time() - start, 2)
            msg = f"[{i}] ❌ 失败 - 错误信息: {e}, 延迟: {elapsed}s"
        print(msg)
        result_lines.append(msg)
        time.sleep(1)

    summary = f"住宅代理成功 {success_count}/10 次"
    print(f"\n{summary}\n")
    result_lines.append(summary)
    return "\n".join(result_lines)


# =============== 住宅代理白名单测试 ================
def test_residential_proxy_whitelist():
    proxy = {
        'http': 'http://pr.thordata.net:9999',
        'https': 'http://pr.thordata.net:9999',
    }
    url = 'http://ipinfo.io'
    success_count = 0
    result_lines = []

    print("=== 住宅代理（白名单）测试开始 ===")
    for i in range(1, 11):
        try:
            start = time.time()
            response = requests.get(url, proxies=proxy, timeout=10)
            elapsed = round(time.time() - start, 2)
            response.raise_for_status()
            data = response.json()
            success_count += 1
            msg = f"[白名单 {i}] ✅ 成功 - IP: {data.get('ip')}, 地区: {data.get('city')}, 国家: {data.get('country')}, 延迟: {elapsed}s"
        except Exception as e:
            elapsed = round(time.time() - start, 2)
            msg = f"[白名单 {i}] ❌ 失败 - 错误信息: {e}, 延迟: {elapsed}s"
        print(msg)
        result_lines.append(msg)
        time.sleep(1)

    summary = f"住宅代理（白名单）成功 {success_count}/10 次"
    print(f"\n{summary}\n")
    result_lines.append(summary)
    return "\n".join(result_lines)


# =============== 无限代理测试 ================
def test_infinite_proxy():
    proxy = {
        'http': 'http://kffa17rt.thordata.online:9999',
        'https': 'http://kffa17rt.thordata.online:9999',
    }
    auth = HTTPProxyAuth('td-customer-pzerSmqmo5uq', '3knb68a0y3')
    url = 'http://ipinfo.io'  # 必须用 HTTP 避免 CONNECT 隧道 407 错误
    success_count = 0
    result_lines = []

    print("=== 无限代理测试开始 ===")
    for i in range(1, 11):
        try:
            start = time.time()
            response = requests.get(url, proxies=proxy, auth=auth, timeout=10)
            elapsed = round(time.time() - start, 2)
            response.raise_for_status()
            data = response.json()
            success_count += 1
            msg = f"[{i}] ✅ 成功 - IP: {data.get('ip')}, 地区: {data.get('city')}, 国家: {data.get('country')}, 延迟: {elapsed}s"
        except Exception as e:
            elapsed = round(time.time() - start, 2)
            msg = f"[{i}] ❌ 失败 - 错误信息: {e}, 延迟: {elapsed}s"
        print(msg)
        result_lines.append(msg)
        time.sleep(1)

    summary = f"无限代理成功 {success_count}/10 次"
    print(f"\n{summary}\n")
    result_lines.append(summary)
    return "\n".join(result_lines)


# =============== 无限代理白名单测试 ================
def test_infinite_proxy_whitelist():
    proxy = {
        'http': 'http://kffa17rt.thordata.online:20000',
        'https': 'http://kffa17rt.thordata.online:20000',
    }
    url = 'http://ipinfo.io'
    success_count = 0
    result_lines = []

    print("=== 无限代理（白名单）测试开始 ===")
    for i in range(1, 11):
        try:
            start = time.time()
            response = requests.get(url, proxies=proxy, timeout=10)
            elapsed = round(time.time() - start, 2)
            response.raise_for_status()
            data = response.json()
            success_count += 1
            msg = f"[白名单 {i}] ✅ 成功 - IP: {data.get('ip')}, 地区: {data.get('city')}, 国家: {data.get('country')}, 延迟: {elapsed}s"
        except Exception as e:
            elapsed = round(time.time() - start, 2)
            msg = f"[白名单 {i}] ❌ 失败 - 错误信息: {e}, 延迟: {elapsed}s"
        print(msg)
        result_lines.append(msg)
        time.sleep(1)

    summary = f"无限代理（白名单）成功 {success_count}/10 次"
    print(f"\n{summary}\n")
    result_lines.append(summary)
    return "\n".join(result_lines)


# =============== ISP / 数据中心代理通用测试 ================
def test_proxy_with_location(proxy_list, proxy_type):
    url = 'http://ipinfo.io'
    results = []
    print(f"=== {proxy_type}测试开始 ===")

    for proxy_str, expected_location in proxy_list:
        ip, port, username, password = proxy_str.split(":")
        proxy_url = f"http://{username}:{password}@{ip}:{port}"
        proxies = {'http': proxy_url, 'https': proxy_url}
        try:
            start = time.time()
            response = requests.get(url, proxies=proxies, timeout=10)
            elapsed = round(time.time() - start, 2)
            response.raise_for_status()
            data = response.json()
            city = data.get("city", "")
            country = data.get("country", "")
            actual_location = f"{country} - {city}"
            if expected_location in actual_location:
                result = "✅ 匹配"
            else:
                result = "❌ 不匹配"
            msg = f"{result} | 预期: {expected_location} | 实际: {actual_location} | IP: {data.get('ip')} | 延迟: {elapsed}s"
        except Exception as e:
            elapsed = round(time.time() - start, 2)
            msg = f"❌ 请求失败 | 代理: {ip}:{port} | 错误信息: {e} | 延迟: {elapsed}s"
        print(msg)
        results.append(msg)
        time.sleep(1)

    print(f"\n{proxy_type}测试完成\n")
    return "\n".join(results)


# =============== 主程序 ================
if __name__ == "__main__":
    # 请替换为你的钉钉Webhook地址
    DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send?access_token=e05a6891a68b9b3b1cfacf8dcf5852bf647457439261362fac0a1e096951bfa9"

    # 各类代理配置
    isp_proxies = [
        ("188.209.143.236:6666:S9VG59Sh0WOx2G4:EMRZFGaXS2EVUo", "US - Portland"),
        ("150.241.167.251:6666:x9kww9dEVGg:qWCeJ6i0SUIWb", "US - Philadelphia - Native"),
        ("45.192.227.186:6666:x9kww9dEVGg:qWCeJ6i0SUIWb", "BR - Sao Paulo"),
        ("167.148.74.16:6666:x9kww9dEVGg:qWCeJ6i0SUIWb", "US - Las Vegas - Native"),
    ]

    dc_proxies = [
        ("154.16.228.141:6667:ce0XtJY5uduZFY:T4OcnuGdZrBPA", "TH - Bangkok"),
        ("38.33.152.196:6667:ce0XtJY5uduZFY:T4OcnuGdZrBPA", "BR - Sao Paulo"),
    ]

    # 执行各类测试
    res1 = test_residential_proxy()
    res1_whitelist = test_residential_proxy_whitelist()
    res2 = test_infinite_proxy()
    res2_whitelist = test_infinite_proxy_whitelist()
    res3 = test_proxy_with_location(isp_proxies, "ISP代理")
    res4 = test_proxy_with_location(dc_proxies, "数据中心代理")

    # 钉钉通知汇总
    report = f"{res1}\n\n{res1_whitelist}\n\n{res2}\n\n{res2_whitelist}\n\n{res3}\n\n{res4}"
    send_dingtalk_report(report, DINGTALK_WEBHOOK)
