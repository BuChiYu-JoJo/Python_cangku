# -*- coding: utf-8 -*-
import asyncio
import datetime
import os
import sys
import random
import uuid
import csv
from pathlib import Path

# === 确保能 import 到工程根目录（thor-serpapi-backend） ===
BASE = os.path.abspath(__file__)
while True:
    BASE = os.path.dirname(BASE)
    if BASE.endswith("thor-serpapi-backend"):
        break
sys.path.append(BASE)

# === 导入你的被测函数 ===
from projects.project_01_tordata.spider_09_yamaxun_fen_version.rep_09_3_6_amazon.rep_09_3_6_amazon_1_2 import (
    amazon_multiple_resp
)

# =========================
# 可 配 置 项
# =========================
NUM_RUNS = 100                    # 本次测试调用多少次
PRODUCT_NAME = "thordata"       # 用于 cos_key 的产品名目录
AMAZON_DOMAIN = "www.amazon.com"  # amazon 域名
PAGE_TURNING_MIN = 1            # 随机翻页下限（包含）
PAGE_TURNING_MAX = 10            # 随机翻页上限（包含）
CSV_OUTPUT = "./amazon_test_runs.csv"  # 结果 CSV 路径（不存在则自动创建）
# 可选：两次调用之间的间隔秒数（避免触发风控，0 表示不等待）
SLEEP_BETWEEN_RUNS = 0

# “产品关键词” 与 “品牌” 的匹配池（确保品牌与品类一致）
PRODUCT_BRAND_CATALOG = {
    # 咖啡相关
    "Coffee": ["Starbucks", "Lavazza", "Nespresso", "Peet's Coffee", "Illy"],
    "Coffee beans": ["Starbucks", "Lavazza", "Peet's Coffee", "Illy"],
    "Espresso": ["Lavazza", "Illy", "Nespresso"],
    "Coffee capsules": ["Nespresso", "Lavazza"],

    # 耳机
    "wireless earbuds": ["Apple", "Sony", "Samsung", "Jabra", "Beats"],
    "headphones": ["Sony", "Bose", "Sennheiser", "Beats", "JBL"],

    # 笔记本
    "laptop": ["Lenovo", "ASUS", "Acer", "HP", "Dell"],
    "gaming laptop": ["ASUS", "Acer", "MSI", "Lenovo"],

    # 牙刷
    "electric toothbrush": ["Oral-B", "Philips Sonicare"],

    # 手机壳
    "iPhone case": ["Spigen", "OtterBox", "ESR", "Caseology"],

    # 水杯
    "water bottle": ["Hydro Flask", "Stanley", "CamelBak", "Nalgene"],
}

# =========================
# 工 具 方 法
# =========================
def random_keyword_brand():
    """从目录中随机选择一组 keyword + brand（保证品牌匹配该类目）"""
    keyword = random.choice(list(PRODUCT_BRAND_CATALOG.keys()))
    brand = random.choice(PRODUCT_BRAND_CATALOG[keyword])
    return keyword, brand

def random_page_turning():
    """随机翻页数"""
    return random.randint(PAGE_TURNING_MIN, PAGE_TURNING_MAX)

def gen_task_id():
    """生成不冲突的 task_id（时间戳 + UUID截断）"""
    ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"task_{ts}_{uid}"

def ensure_csv_header(csv_path: str, header: list):
    """如果 CSV 文件不存在，则写入表头"""
    csv_file = Path(csv_path)
    if not csv_file.exists():
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)

def append_csv_row(csv_path: str, row: list):
    """追加一行 CSV"""
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)

# =========================
# 主 逻 辑
# =========================
async def run_once():
    """执行一次完整调用：随机参数 -> 调用 -> 返回记录"""
    # 1) 随机构造参数
    keyword, brand = random_keyword_brand()
    page_turning = random_page_turning()
    task_id = gen_task_id()
    current_date = datetime.datetime.now()

    # 2) 构造 cos_key
    cos_key = "scrapers/{}/{}/{}".format(
        PRODUCT_NAME,
        current_date.strftime("%Y/%m/%d"),
        task_id
    )

    # 3) 组装 spider_inputs
    spider_inputs = {
        "spider_errors": True,  # 如果希望不强制标错，可改为 False
        "spider_id": "amazon_global-product_by-keywords-brand",
        "spider_parameters": [
            {
                "keyword": keyword,
                "brands": brand,
                "domain": AMAZON_DOMAIN,
                "page_turning": page_turning
            }
        ]
    }

    # 4) 错误容器必须是 set（函数内部会用 .add）
    error = set()

    # 5) 调用被测方法
    result_num, total_time, size_in_bytes, error_number, error = await amazon_multiple_resp(
        spider_inputs,
        cos_key,
        error
    )

    # 6) 组织记录信息
    now_iso = datetime.datetime.now().isoformat(timespec="seconds")
    error_str = "; ".join(sorted(map(str, error))) if error else ""
    record = {
        "timestamp": now_iso,
        "task_id": task_id,
        "keyword": keyword,
        "brand": brand,
        "page_turning": page_turning,
        "result_num": result_num,
        "total_time": round(float(total_time), 4),
        "size_in_bytes": int(size_in_bytes),
        "error_number": int(error_number),
        "error": error_str,
        "cos_key": cos_key,
    }
    return record

async def main():
    # 准备 CSV
    header = [
        "timestamp",
        "task_id",
        "keyword",
        "brand",
        "page_turning",
        "result_num",
        "total_time",
        "size_in_bytes",
        "error_number",
        "error",
        "cos_key",
    ]
    ensure_csv_header(CSV_OUTPUT, header)

    print(f"Starting test runs: NUM_RUNS={NUM_RUNS}")
    all_records = []

    for i in range(1, NUM_RUNS + 1):
        print(f"\n=== Run {i}/{NUM_RUNS} ===")
        try:
            record = await run_once()
        except Exception as e:
            # 捕获异常也写入一条失败记录，保证每轮都有结果
            now_iso = datetime.datetime.now().isoformat(timespec="seconds")
            task_id = gen_task_id()
            record = {
                "timestamp": now_iso,
                "task_id": task_id,
                "keyword": "N/A",
                "brand": "N/A",
                "page_turning": "N/A",
                "result_num": 0,
                "total_time": 0,
                "size_in_bytes": 0,
                "error_number": 1,
                "error": f"runner-exception: {e}",
                "cos_key": f"scrapers/{PRODUCT_NAME}/{datetime.datetime.now().strftime('%Y/%m/%d')}/{task_id}",
            }

        # 打印与写入
        print("Result statistics:")
        print("  task_id      :", record["task_id"])
        print("  keyword/brand:", f"{record['keyword']} / {record['brand']}")
        print("  page_turning :", record["page_turning"])
        print("  result_num   :", record["result_num"])
        print("  total_time   :", record["total_time"])
        print("  size_in_bytes:", record["size_in_bytes"])
        print("  error_number :", record["error_number"])
        print("  error        :", record["error"])
        print("  cos_key      :", record["cos_key"])

        append_csv_row(CSV_OUTPUT, [
            record["timestamp"],
            record["task_id"],
            record["keyword"],
            record["brand"],
            record["page_turning"],
            record["result_num"],
            record["total_time"],
            record["size_in_bytes"],
            record["error_number"],
            record["error"],
            record["cos_key"],
        ])
        all_records.append(record)

        if SLEEP_BETWEEN_RUNS > 0 and i < NUM_RUNS:
            await asyncio.sleep(SLEEP_BETWEEN_RUNS)

    print(f"\nAll done. CSV saved to: {os.path.abspath(CSV_OUTPUT)}")
    print(f"Total runs: {len(all_records)}")

if __name__ == "__main__":
    asyncio.run(main())
