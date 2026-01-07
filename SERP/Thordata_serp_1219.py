#!/usr/bin/env python3
"""
SerpAPI Performance Test Script
Tests SerpAPI service with configurable engines, concurrency, and detailed performance metrics.
"""

import http.client
import csv
import time
import json
import argparse
import concurrent.futures
import random
import itertools
import os
from urllib.parse import urlencode, urlparse
from datetime import datetime
from collections import defaultdict
import ssl
import math
import threading


class SerpAPITester:
    """SerpAPI性能测试类"""

    # SerpAPI支持的所有引擎（与最新分支保持一致）
    SUPPORTED_ENGINES = [
        'google', 'google_local', 'google_images', 'google_videos',
        'google_news', 'google_shopping', 'google_play', 'google_jobs',
        'google_scholar', 'google_finance', 'google_patents', 'google_lens',
        'google_flights', 'google_trends', 'google_hotels', 'google_maps',
        'google_ai_mode',

        # 新增通用搜索引擎支持（使用默认参数池/默认行为）
        'bing', 'bing_images', 'bing_videos', 'bing_news',
        'bing_maps', 'bing_shopping',

        'yandex', 'duckduckgo'
    ]

    def __init__(self, api_key, save_details=False):
        """
        初始化SerpAPI测试器

        Args:
            api_key: SerpAPI认证密钥
            save_details: 是否保存每个请求的详细CSV记录
        """
        self.api_key = api_key
        self.host = "scraperapi.thordata.com"
        self.save_details = save_details
        # 默认关键词池，当引擎未配置专属关键词时回退使用
        self.keyword_pool = [
            "pizza", "coffee", "restaurant", "weather", "news",
            "hotel", "flight", "car", "phone", "laptop",
            "book", "music", "movie", "game", "sport",
            "health", "fitness", "recipe", "travel", "shopping",
            "weather tomorrow", "nearby restaurants", "best cafes",
            "smartwatch", "headphones", "tablet", "camera",
            "electric car", "used cars", "car rental",
            "cheap flights", "flight status", "airport",
            "luxury hotel", "hostel", "airbnb",
            "stock market", "bitcoin", "currency exchange",
            "technology", "ai news", "space exploration",
            "basketball", "football", "tennis",
            "concert", "festival", "museum",
            "shopping mall", "discounts", "coupons",
            "recipes easy", "vegan recipes", "healthy meals",
            "pharmacy", "clinic near me", "dentist",
            "fitness gym", "workout plan", "yoga",
            "mobile games", "pc games", "game reviews",
            "movies 2025", "tv shows", "cartoon",
            "books best seller", "novels", "ebooks"
        ]

        # 参考 SerpApi 测试脚本：为不同引擎配置更贴合场景的关键词
        self.engine_keywords = {
            "google_play": [
                "productivity app", "fitness tracker app",
                "photo editor", "music streaming app", "language learning app",
                "budget tracker", "habit tracker", "calendar app",
                "travel planner app", "weather forecast app"
            ],
            "google_jobs": [
                "software engineer", "data scientist", "product manager",
                "ux designer", "marketing manager", "cloud architect",
                "devops engineer", "qa engineer", "project manager",
                "accountant"
            ],
            "google_scholar": [
                "machine learning", "quantum computing", "climate change",
                "computer vision", "natural language processing",
                "renewable energy", "graph neural networks",
                "blockchain security", "genome sequencing", "edge computing"
            ],
            "google_finance": [
                "AAPL stock", "TSLA stock", "MSFT stock",
                "GOOGL stock", "AMZN stock", "NVDA stock",
                "USD to EUR", "NASDAQ index", "Dow Jones",
                "S&P 500"
            ],
            "google_patents": [
                "electric vehicle battery", "solar panel efficiency",
                "3d printing metal", "autonomous driving system",
                "drone delivery", "medical imaging device",
                "wireless charging", "vr headset optics",
                "robotic arm control", "quantum encryption"
            ],
            "google_lens": [
                "https://i.imgur.com/HBrB8p0.png",
                "https://picsum.photos/800/500",
                "https://picsum.photos/600/400",
                "https://picsum.photos/300/300",
                "https://picsum.photos/1200/800",
                "https://picsum.photos/1080/720",
                "https://loremflickr.com/800/600",
                "https://loremflickr.com/640/480",
                "https://loremflickr.com/1024/768",
                "https://loremflickr.com/500/600",
                "https://loremflickr.com/1200/900",
                "https://images.unsplash.com/photo-1503023345310-bd7c1de61c7d",
                "https://images.unsplash.com/photo-1529626455594-4ff0802cfb7e",
                "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee",
                "https://images.unsplash.com/photo-1519682577862-22b62b24e493",
                "https://images.unsplash.com/photo-1524504388940-b1c1722653e1"
            ],
            "google_flights": [
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-07",
                    "return_date": "2025-12-09",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-08",
                    "return_date": "2025-12-10",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-09",
                    "return_date": "2025-12-11",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-11",
                    "return_date": "2025-12-13",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-13",
                    "return_date": "2025-12-15",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-27",
                    "return_date": "2025-12-30",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-07",
                    "return_date": "2025-12-15",
                    "currency": "USD"
                }
            ],
            "google_hotels": [
                {
                    "q": "Bali Resorts",
                    "check_in_date": "2025-12-17",
                    "check_out_date": "2025-12-18"
                },
                {
                    "q": "Tokyo luxury hotels",
                    "check_in_date": "2025-12-15",
                    "check_out_date": "2025-12-20"
                },
                {
                    "q": "New York boutique hotels",
                    "check_in_date": "2025-12-22",
                    "check_out_date": "2025-12-26"
                },
                {
                    "q": "Paris family hotels",
                    "check_in_date": "2026-01-05",
                    "check_out_date": "2026-01-09"
                },
                {
                    "q": "Sydney beach resorts",
                    "check_in_date": "2026-02-10",
                    "check_out_date": "2026-02-15"
                }
            ],
            "google_trends": [
                {
                    "q": "coffee",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "milk",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "bread",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "pasta",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "steak",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "ai",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "vr",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "5g",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "cloud",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "python,java",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "go,rust",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "nba,ufc",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "bitcoin",
                    "data_type": "TIMESERIES"
                }
            ],
            "google_maps": [
                {
                    "q": "pizza",
                    "type": "search"
                },
                {
                    "q": "coffee",
                    "type": "search"
                },
                {
                    "q": "restaurant",
                    "type": "search"
                },
                {
                    "q": "hotel",
                    "type": "search"
                },
                {
                    "q": "gym",
                    "type": "search"
                }
            ],
            # bing_maps 关键词池：单个词（不再是组合短语）
            "bing_maps": [
                "restaurant",
                "coffee",
                "gas",
                "hospital",
                "parking",
                "ev",
                "theater",
                "gym",
                "hotels",
                "museums",
                "transit",
                "pharmacy",
                "airport",
                "mall",
                "bike"
            ],
            # bing_shopping 关键词池：单个词商品/品牌/类目
            "bing_shopping": [
                "headphones",
                "tv",
                "laptop",
                "nike",
                "smartphone",
                "bicycle",
                "coffee",
                "chair",
                "camera",
                "stroller",
                "beans",
                "jacket",
                "sneakers",
                "smartwatch",
                "charger"
            ]
        }

    def make_request(self, engine, query):
        """
        发送单个API请求并测量准确的响应时间

        Args:
            engine: 搜索引擎名称
            query: 搜索关键词

        Returns:
            dict: 包含请求结果的字典
        """
        result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'product': 'Thordata',
            'engine': engine,
            'query': json.dumps(query, ensure_ascii=False) if isinstance(query, dict) else query,
            'status_code': None,
            'response_time': None,
            'response_size': None,
            'success': False,
            'error': '',
            'response_excerpt': ''
        }

        conn = None
        #Thordata的请求方式
        try:
            params = {
                "engine": engine,
                "json": "1",
                "no_cache": "true"
            }

            # 根据不同引擎处理查询参数
            if engine == "google_lens":
                params["url"] = query
            # yandex 使用 text 参数
            elif engine == "yandex":
                params["text"] = query
            # 以下引擎需要 dict 型 query 并将其展开为参数
            elif engine in {"google_flights", "google_trends", "google_hotels", "google_maps"}:
                if not isinstance(query, dict):
                    raise ValueError(f"{engine} 查询参数必须为字典类型")
                if engine in {"google_trends", "google_hotels"} and not query.get("q"):
                    raise ValueError(f"{engine} 参数缺少必填项: q")
                if engine == "google_maps" and not query.get("type"):
                    raise ValueError(f"{engine} 参数缺少必填项: type")
                params.update(query)
            else:
                # 默认使用 q 参数（其他新增引擎如 bing/duckduckgo 使用默认 q）
                params["q"] = query

            payload = urlencode(params)

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/x-www-form-urlencoded"
            }

            path = "/request"
            conn = http.client.HTTPSConnection(self.host, timeout=60)

            start_time = time.perf_counter()
            conn.request("POST", path, payload, headers)

            response = conn.getresponse()
            data = response.read()
            end_time = time.perf_counter()

            result['response_time'] = round(end_time - start_time, 3)
            result['status_code'] = response.status
            result['response_size'] = round(len(data) / 1024, 3)

            # ---- ★ 修复：Thordata 响应是 JSON 字符串，需要 double JSON decode ★ ----
            text = data.decode("utf-8", errors="ignore")
            result['response_excerpt'] = text[:1000]

            try:
                # 第一次解析：从原始文本 -> JSON 字符串
                first_parsed = json.loads(text)

                # 如果结果仍是字符串，说明还需要第二次解析
                if isinstance(first_parsed, str):
                    response_json = json.loads(first_parsed)
                else:
                    response_json = first_parsed

            except Exception as e:
                result['success'] = False
                result['error'] = f"Non-JSON response: {str(e)}"
                return result

            # ---- ★ JSON 一定是 dict，到这里保持 dict，不再覆盖 ★ ----
            result['success'] = self._is_response_successful(response_json, response.status)

            if not result['success']:
                result['error'] = self._extract_error_message(response_json)

            return result

        except Exception as e:
            result['success'] = False
            result['error'] = f"Request error: {str(e)}"
            if 'start_time' in locals():
                result['response_time'] = round(time.perf_counter() - start_time, 3)
            return result

        finally:
            if conn:
                conn.close()

        return result

    def _is_response_successful(self, response_json, status_code):

        # HTTP 必须是 200
        if status_code != 200:
            return False

        # Thordata 成功标记
        if response_json.get("search_metadata", {}).get("status") == "Success":
            return True

        # SerpAPI 风格字段兼容
        result_fields = [
            "organic_results",
            "shopping_results",
            "news_results",
            "images_results",
            "videos_results",
            "local_results",
            "ai_overview",
            "search_information",
        ]
        return any(field in response_json for field in result_fields)

    def _extract_error_message(self, response_json):

        if isinstance(response_json, dict):

            # Thordata 的错误字段（只有失败情况才会出现）
            if "error" in response_json:
                return response_json["error"]

            # 部分接口会将错误信息放在 data 字段
            if "data" in response_json:
                data_value = response_json["data"]
                code_value = response_json.get("code")

                def _to_string(value):
                    if isinstance(value, (str, int, float)):
                        return str(value)
                    try:
                        return json.dumps(value, ensure_ascii=False)
                    except Exception:
                        return None

                data_message = _to_string(data_value)

                if data_message and code_value is not None:
                    return f"code:{code_value}, {data_message}"
                if data_message:
                    return data_message

            # 非成功：检查 search_metadata 状态
            status = response_json.get("search_metadata", {}).get("status")
            if status and status != "Success":
                return f"Status: {status}"

            return "No error field found"

        # 理论上不会发生，但放在这里兜底
        return "Invalid JSON structure"

    def _extract_response_summary(self, response_json):
        """
        提取响应摘要信息

        Args:
            response_json: 解析后的JSON响应

        Returns:
            str: 响应摘要
        """
        summary_parts = []

        # 提取搜索信息
        if 'search_information' in response_json:
            info = response_json['search_information']
            if 'total_results' in info:
                summary_parts.append(f"total_results:{info['total_results']}")

        # 统计各类结果数量
        if 'organic_results' in response_json:
            summary_parts.append(f"organic:{len(response_json['organic_results'])}")
        if 'shopping_results' in response_json:
            summary_parts.append(f"shopping:{len(response_json['shopping_results'])}")
        if 'images_results' in response_json:
            summary_parts.append(f"images:{len(response_json['images_results'])}")

        return ", ".join(summary_parts) if summary_parts else "Success"

    def _get_next_query_fn(self, engine, explicit_query):
        """
        为给定引擎创建获取下一个查询的函数，支持显式查询、引擎专属关键词和轮询关键词池。
        """
        if explicit_query is not None:
            return lambda: explicit_query

        engine_pool = self.engine_keywords.get(engine)
        if engine_pool:
            return lambda: random.choice(engine_pool)

        pool = self.keyword_pool
        cycle_iter = itertools.cycle(pool)
        lock = threading.Lock()

        def round_robin_query():
            with lock:
                return next(cycle_iter)

        return round_robin_query

    def run_concurrent_test(self, engine, duration_seconds, concurrency, query=None,
                           target_qps=None, max_requests=None):
        """
        运行并发性能测试

        Args:
            engine: 搜索引擎名称
            duration_seconds: 运行时长（秒）
            concurrency: 并发数
            query: 搜索关键词（可选，默认随机）

        Returns:
            tuple: (所有请求结果, 总耗时, 总请求数)
        """
        if concurrency <= 0:
            raise ValueError("并发数必须大于0")
        results = []
        start_monotonic = time.perf_counter()
        end_time = start_monotonic + duration_seconds
        next_query_fn = self._get_next_query_fn(engine, query)
        request_counter = {"count": 0} if max_requests else None
        counter_lock = threading.Lock() if max_requests else None

        print(f"\n开始测试引擎: {engine}")
        print(f"  运行时间: {duration_seconds}秒")
        print(f"  并发数: {concurrency}")
        print(f"  缓存: 禁用 (no_cache=true)")
        if target_qps or max_requests:
            print(f"  目标QPS: {target_qps if target_qps else 'Unlimited'}")
            if max_requests:
                print(f"  最大请求数: {max_requests}")
        print("-" * 80)

        # 记录并发测试的总开始时间
        total_start_time = start_monotonic

        # 使用线程池进行并发测试
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            # 提交并发工作线程
            futures = [
                executor.submit(
                    self._run_worker_until,
                    engine,
                    next_query_fn,
                    end_time,
                    concurrency,
                    target_qps,
                    max_requests,
                    request_counter,
                    counter_lock
                )
                for _ in range(concurrency)
            ]

            # 收集结果
            for idx, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    worker_results = future.result()
                    results.extend(worker_results)
                except Exception as e:
                    print(f"  线程 {idx} 异常: {str(e)}")

        # 记录并发测试的总结束时间
        total_end_time = time.perf_counter()
        total_duration = round(total_end_time - total_start_time, 3)
        total_requests = len(results)

        print(f"\n并发测试完成，总耗时: {total_duration}秒，总请求数: {total_requests}")

        return results, total_duration, total_requests

    def _run_worker_until(self, engine, next_query_fn, end_time, concurrency,
                          target_qps=None, max_requests=None, request_counter=None, counter_lock=None):
        """
        Worker 线程：在截止时间前持续发送请求，不再新增超时请求
        """
        worker_results = []
        if target_qps and target_qps > 0:
            worker_qps = target_qps / concurrency
            min_interval = 1.0 / worker_qps
        else:
            worker_qps = None
            min_interval = 0
        last_request_time = time.perf_counter()
        next_allowed_time = last_request_time + min_interval if worker_qps else None

        def reserve_request():
            if not max_requests or request_counter is None or counter_lock is None:
                return True
            with counter_lock:
                request_counter["count"] += 1
                return request_counter["count"] <= max_requests

        current_time = time.perf_counter()
        if current_time >= end_time:
            return worker_results

        while True:
            now = time.perf_counter()
            if worker_qps:
                if next_allowed_time is not None and now < next_allowed_time:
                    time.sleep(next_allowed_time - now)
                    now = time.perf_counter()
                next_allowed_time = now + min_interval

            last_request_time = now
            if last_request_time >= end_time:
                break

            if not reserve_request():
                break

            result = self.make_request(engine, next_query_fn())
            worker_results.append(result)

            current_time = time.perf_counter()
            if current_time >= end_time:
                break
        return worker_results

    def run_all_engines_test(self, engines, duration_seconds, concurrency,
                             query=None, target_qps=None, max_requests=None):
        """
        测试多个引擎的性能

        Args:
            engines: 要测试的引擎列表
            duration_seconds: 每个引擎的运行时长（秒）
            concurrency: 并发数

        Returns:
            dict: 所有引擎的测试结果
        """
        all_results = {}
        all_statistics = []

        print("=" * 80)
        print("开始批量引擎性能测试")
        print("=" * 80)

        for engine in engines:
            try:
                results, total_duration, total_requests = self.run_concurrent_test(
                    engine, duration_seconds, concurrency, query,
                    target_qps, max_requests
                )

                all_results[engine] = results

                # 计算统计数据
                stats = self._calculate_statistics(
                    'Thordata', engine, results, total_requests,
                    concurrency, total_duration, target_qps
                )
                all_statistics.append(stats)

                # 如果启用详细记录，保存CSV
                if self.save_details:
                    self._save_detailed_csv(engine, results, concurrency)

            except Exception as e:
                print(f"\n引擎 {engine} 测试失败: {str(e)}")
                continue

        return all_results, all_statistics

    def _compute_error_rate(self, success_count, total_requests):
        if total_requests <= 0:
            return 0
        return round((total_requests - success_count) / total_requests * 100, 2)

    def _calculate_statistics(self, product, engine, results, total_requests,
                              concurrency, total_duration, target_qps=None):
        """
        计算统计数据

        Returns:
            dict: 统计数据
        """
        successful_results = [r for r in results if r['success']]
        failed_results = [r for r in results if not r['success']]

        success_count = len(successful_results)
        success_rate = round(success_count / total_requests * 100, 2) if total_requests > 0 else 0

        # 计算成功请求的平均响应时间
        avg_response_time = 0
        if successful_results:
            total_time = sum(r['response_time'] for r in successful_results if r['response_time'])
            avg_response_time = round(total_time / len(successful_results), 3)

        # 计算P50、P75、P90、P95、P99延迟
        p50_latency = 0
        p75_latency = 0
        p90_latency = 0
        p95_latency = 0
        p99_latency = 0
        if successful_results:
            response_times = sorted([r['response_time'] for r in successful_results if r['response_time']])
            if response_times:
                # 使用ceil计算百分位索引
                def get_percentile_value(times, percentile):
                    index = math.ceil(len(times) * percentile) - 1  # -1因为索引从0开始
                    if index < 0:
                        index = 0
                    if index >= len(times):
                        index = len(times) - 1
                    return round(times[index], 3)

                p50_latency = get_percentile_value(response_times, 0.5)
                p75_latency = get_percentile_value(response_times, 0.75)
                p90_latency = get_percentile_value(response_times, 0.9)
                p95_latency = get_percentile_value(response_times, 0.95)
                p99_latency = get_percentile_value(response_times, 0.99)

        # 计算请求速率 (请求/秒)
        request_rate = round(total_requests / total_duration, 3) if total_duration > 0 else 0
        error_rate = self._compute_error_rate(success_count, total_requests)

        # 计算成功请求的平均响应大小
        avg_response_size = 0
        if successful_results:
            total_size = sum(r['response_size'] for r in successful_results if r['response_size'])
            avg_response_size = round(total_size / len(successful_results), 3)

        stats = {
            '产品类别': product,
            '引擎': engine,
            '请求总数': total_requests,
            '并发数': concurrency,
            '请求速率(req/s)': request_rate,
            '目标QPS': target_qps if target_qps else 'Unlimited',
            '成功次数': success_count,
            '成功率(%)': success_rate,
            '错误率(%)': error_rate,
            '成功平均响应时间(s)': avg_response_time,
            'P50延迟(s)': p50_latency,
            'P75延迟(s)': p75_latency,
            'P90延迟(s)': p90_latency,
            'P95延迟(s)': p95_latency,
            'P99延迟(s)': p99_latency,
            '并发完成时间(s)': total_duration,
            '成功平均响应大小(KB)': avg_response_size
        }

        return stats

    def _normalize_timestamp_str(self, ts):
        """
        将各种格式的时间值标准化为 YYYY-MM-DD HH:MM:SS
        无法解析时返回空字符串以避免写入错误格式
        """
        if ts is None:
            return ""
        # 数字时间戳
        if isinstance(ts, (int, float)):
            try:
                return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return ""
        # 字符串或其他
        s = str(ts).strip()
        if not s:
            return ""
        # 统一分隔符
        s = s.replace("/", "-")
        # 补秒
        if len(s.split(":")) == 2:
            s = f"{s}:00"
        try:
            return datetime.fromisoformat(s).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            try:
                return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return ""

    def _save_detailed_csv(self, engine, results, concurrency):
        """
        保存详细的请求记录到CSV

        Args:
            engine: 引擎名称
            results: 请求结果列表
            concurrency: 并发数，用于文件名区分
        """
        filename = f"thordata_{engine}_c{concurrency}_detailed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        fieldnames = [
            'timestamp', 'product', 'engine', 'query', 'status_code',
            'response_time', 'response_size', 'success', 'error', 'response_excerpt'
        ]

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            formatted_results = []
            for r in results:
                row = dict(r)
                ts = row.get('timestamp')
                normalized_ts = self._normalize_timestamp_str(ts)
                row['timestamp'] = normalized_ts
                formatted_results.append(row)
            writer.writerows(formatted_results)
            csvfile.flush()
            os.fsync(csvfile.fileno())

        print(f"  详细记录已保存到: {filename}")

    def save_summary_statistics(self, statistics, filename='thordata_summary_statistics.csv'):
        """
        保存汇总统计表

        Args:
            statistics: 统计数据列表
            filename: 输出文件名
        """
        if not statistics:
            print("没有统计数据可保存")
            return

        fieldnames = [
            '产品类别', '引擎', '请求总数', '并发数', '请求速率(req/s)', '目标QPS',
            '成功次数', '成功率(%)', '错误率(%)', '成功平均响应时间(s)',
            'P50延迟(s)', 'P75延迟(s)', 'P90延迟(s)', 'P95延迟(s)', 'P99延迟(s)',
            '并发完成时间(s)', '成功平均响应大小(KB)'
        ]

        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(statistics)
            csvfile.flush()
            os.fsync(csvfile.fileno())

        print(f"\n{'=' * 80}")
        print(f"汇总统计表已保存到: {filename}")
        print(f"{'=' * 80}")

        # 打印统计表
        self._print_statistics_table(statistics)

    def _print_statistics_table(self, statistics):
        """
        在控制台打印统计表

        Args:
            statistics: 统计数据列表
        """
        print("\n汇总统计表:")
        header = f"{'引擎':<20} {'请求数':>8} {'并发':>6} {'速率(req/s)':>12} " \
                 f"{'目标QPS':>12} {'成功':>8} {'成功率':>8} {'错误率':>8} {'平均响应(s)':>12} " \
                 f"{'P50延迟(s)':>11} {'P75延迟(s)':>11} {'P90延迟(s)':>11} {'P95延迟(s)':>11} {'P99延迟(s)':>11} " \
                 f"{'完成时间(s)':>12} {'响应大小(KB)':>14}"
        table_width = len(header)
        print("-" * table_width)
        print(header)
        print("-" * table_width)

        # 打印数据行
        for stat in statistics:
            row = f"{stat['引擎']:<20} {stat['请求总数']:>8} {stat['并发数']:>6} " \
                  f"{stat['请求速率(req/s)']:>12} {stat['目标QPS']:>12} {stat['成功次数']:>8} " \
                  f"{stat['成功率(%)']:>7}% {stat['错误率(%)']:>7}% {stat['成功平均响应时间(s)']:>12} " \
                  f"{stat['P50延迟(s)']:>11} {stat['P75延迟(s)']:>11} {stat['P90延迟(s)']:>11} {stat['P95延迟(s)']:>11} {stat['P99延迟(s)']:>11} " \
                  f"{stat['并发完成时间(s)']:>12} {stat['成功平均响应大小(KB)']:>14}"
            print(row)

        print("-" * table_width)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='SerpAPI性能测试脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 测试单个引擎
  python serpapi_test.py -k YOUR_API_KEY -e google -t 60 -c 5

  # 测试多个引擎
  python serpapi_test.py -k YOUR_API_KEY -e google bing yahoo -t 120 -c 10

  # 测试所有引擎
  python serpapi_test.py -k YOUR_API_KEY --all-engines -t 60 -c 5

  # 启用详细CSV记录
  python serpapi_test.py -k YOUR_API_KEY -e google -t 60 -c 5 --save-details
        """
    )

    parser.add_argument('-k', '--api-key', type=str,
                        help='SerpAPI认证密钥')
    parser.add_argument('-e', '--engines', type=str, nargs='+',
                        help='要测试的搜索引擎列表')
    parser.add_argument('--all-engines', action='store_true',
                        help='测试所有支持的引擎')
    parser.add_argument('-t', '--duration', type=int, default=60,
                        help='每个引擎的运行时间(秒) (默认: 60)')
    parser.add_argument('-c', '--concurrency', type=int, default=5,
                        help='并发数 (默认: 5)')
    parser.add_argument('--concurrency-steps', type=int, nargs='+',
                        help='连续执行的并发列表，例如: --concurrency-steps 20 50')
    parser.add_argument('-q', '--query', type=str,
                        help='搜索关键词 (默认: 随机)')
    parser.add_argument('--save-details', action='store_true',
                        help='保存每个请求的详细CSV记录')
    parser.add_argument('--target-qps', type=float,
                        help='目标请求速率 (QPS)，设置后按QPS节流')
    parser.add_argument('--max-requests', type=int,
                        help='最大请求总数，与目标QPS组合以控制成本')
    parser.add_argument('-o', '--output', type=str,
                        default='serpapi_summary_statistics.csv',
                        help='汇总统计表输出文件名')
    parser.add_argument('--list-engines', action='store_true',
                        help='列出所有支持的引擎')

    args = parser.parse_args()

    # 列出所有支持的引擎
    if args.list_engines:
        print("支持的搜索引擎:")
        for i, engine in enumerate(SerpAPITester.SUPPORTED_ENGINES, 1):
            print(f"  {i:2d}. {engine}")
        return

    # 验证API密钥
    if not args.api_key:
        print("错误: 请使用 -k 或 --api-key 指定API密钥")
        return

    # 确定要测试的引擎
    if args.all_engines:
        engines = SerpAPITester.SUPPORTED_ENGINES
        print(f"将测试所有 {len(engines)} 个引擎")
    elif args.engines:
        engines = args.engines
        # 验证引擎是否支持
        invalid_engines = [e for e in engines if e not in SerpAPITester.SUPPORTED_ENGINES]
        if invalid_engines:
            print(f"警告: 以下引擎不在支持列表中: {', '.join(invalid_engines)}")
            print("使用 --list-engines 查看支持的引擎列表")
    else:
        print("错误: 请使用 -e 指定引擎或使用 --all-engines 测试所有引擎")
        print("使用 --list-engines 查看支持的引擎列表")
        return

    # 创建测试器
    tester = SerpAPITester(args.api_key, save_details=args.save_details)

    if args.target_qps or args.max_requests:
        print("\n=== 经济模式已启用 ===")
        if args.target_qps:
            print(f"目标 QPS: {args.target_qps}")
        if args.max_requests:
            print(f"最大请求数: {args.max_requests}")
        print("====================\n")

    # 运行测试：支持连续并发配置
    concurrency_list = args.concurrency_steps if args.concurrency_steps else [args.concurrency]
    all_results = {}
    all_statistics = []

    for conc in concurrency_list:
        print(f"\n==== 开始并发 {conc} 的测试 ====")
        results, statistics = tester.run_all_engines_test(
            engines, args.duration, conc, args.query,
            args.target_qps, args.max_requests
        )
        # 合并结果
        all_results.update(results)
        all_statistics.extend(statistics)

    # 保存汇总统计
    tester.save_summary_statistics(all_statistics, args.output)

    print("\n测试完成!")


if __name__ == "__main__":
    main()
