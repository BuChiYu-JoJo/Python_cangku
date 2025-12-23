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
from urllib.parse import urlencode
from datetime import datetime
import math


class SerpAPITester:
    """SerpAPI性能测试类"""

    # SerpAPI支持的所有引擎
    SUPPORTED_ENGINES = [
        'google', 'google_local', 'google_images', 'google_videos',
        'google_news', 'google_shopping', 'google_play', 'google_jobs',
        'google_scholar', 'google_finance',
        'google_patents', 'google_lens',
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
        self.host = "serpapi.com"
        self.save_details = save_details
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
        self.engine_keyword_map = {
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
                    "outbound_date": "2025-12-05",
                    "return_date": "2025-12-06",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-08",
                    "return_date": "2025-12-12",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-10",
                    "return_date": "2025-12-13",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-12",
                    "return_date": "2025-12-15",
                    "currency": "USD"
                },
                {
                    "hl": "en",
                    "gl": "us",
                    "departure_id": "PEK",
                    "arrival_id": "AUS",
                    "outbound_date": "2025-12-14",
                    "return_date": "2025-12-16",
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
                    "outbound_date": "2025-12-20",
                    "return_date": "2025-12-23",
                    "currency": "USD"
                }
            ],
            "google_hotels": [
                {
                    "q": "Bali Resorts",
                    "check_in_date": "2025-12-27",
                    "check_out_date": "2025-12-28"
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
                    "q": "coffee,milk,bread,pasta,steak",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "ai,blockchain,cloud,vr,5g",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "python,java,go,rust,typescript",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "nba,nfl,mlb,nhl,ufc",
                    "data_type": "TIMESERIES"
                },
                {
                    "q": "bitcoin,ethereum,solana,cardano,ripple",
                    "data_type": "TIMESERIES"
                }
            ],
            "google_maps": [
                {"q": "pizza", "type": "search"},
                {"q": "coffee", "type": "search"},
                {"q": "restaurant", "type": "search"},
                {"q": "hotel", "type": "search"},
                {"q": "gym", "type": "search"}
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

        注意：按你的要求【不要改变原有的请求参数构造】，这里保持原逻辑：
        - params: engine/api_key/no_cache
        - google_lens => url
        - yandex => text
        - 特殊引擎 dict query => params.update(query)
        - 其他 => q
        - path: /search?{urlencode(params)}
        - GET 请求
        """
        result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'product': 'SerpAPI',
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
        try:
            # 准备请求参数 - 禁用缓存以获取真实响应时间
            params = {
                "engine": engine,
                "api_key": self.api_key,
                "no_cache": "true"  # 禁用缓存
            }

            # google_lens 使用 url 参数
            if engine == "google_lens":
                params["url"] = query
            # yandex 使用 text 参数（按照你的要求）
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

            path = f"/search?{urlencode(params)}"

            # 创建HTTPS连接
            conn = http.client.HTTPSConnection(self.host, timeout=30)

            # 记录请求开始时间（仅测量网络请求时间）
            start_time = time.perf_counter()

            # 发送GET请求
            conn.request("GET", path)

            # 获取响应
            response = conn.getresponse()

            # 读取响应数据
            data = response.read()

            # 记录响应结束时间（不包括后续处理时间）
            end_time = time.perf_counter()
            result['response_time'] = round(end_time - start_time, 3)

            result['status_code'] = response.status
            result['response_size'] = round(len(data) / 1024, 3)  # KB

            # 解析响应判断是否成功
            try:
                response_json = json.loads(data.decode('utf-8'))
                result['success'] = self._is_response_successful(response_json, response.status)

                # 保存部分真实响应内容（前1000字符）
                response_text = json.dumps(response_json, ensure_ascii=False)
                result['response_excerpt'] = response_text[:1000]

                # 如果失败，记录错误信息
                if not result['success']:
                    result['error'] = self._extract_error_message(response_json)

            except json.JSONDecodeError as e:
                result['success'] = False
                result['error'] = f"JSON decode error: {str(e)}"
                result['response_excerpt'] = data.decode('utf-8', errors='ignore')[:200]
            except Exception as e:
                result['success'] = False
                result['error'] = f"Response parse error: {str(e)}"

        except Exception as e:
            result['error'] = str(e)
            result['success'] = False
            # 如果请求过程中出错，仍然记录耗时
            if 'start_time' in locals():
                result['response_time'] = round(time.perf_counter() - start_time, 3)
        finally:
            if conn:
                conn.close()

        return result

    def _is_response_successful(self, response_json, status_code):
        """
        判断SerpAPI响应是否成功

        成功条件:
        1. HTTP状态码为200
        2. 响应JSON不包含error字段
        3. 响应包含搜索结果数据
        """
        # 检查HTTP状态码
        if status_code != 200:
            return False

        # 检查是否有错误字段
        if 'error' in response_json:
            return False

        # 检查是否包含搜索结果（不同引擎的结果字段可能不同）
        result_fields = [
            'organic_results', 'shopping_results', 'images_results',
            'video_results', 'news_results', 'local_results',
            'answer_box', 'knowledge_graph', 'flights_results', 'flights', 'layovers',
            'other_flights', 'airports', 'arrival', 'best_flights',
            'jobs_results', 'scholar_results', 'search_information',
            'patent_results', 'app_results', 'finance_results',
            'markets', 'top_stories', 'visual_matches', 'best_flights',
            'interest_over_time', 'brands', 'properties', 'prices', 'nearby_places',
            'quick_results', 'text_blocks', 'reference_indexes', 'references',
            # 兼容字段补充（与 Thordata 统计口径更贴近，不改变原逻辑含义）
            'ai_overview'
        ]

        # 只要包含任一结果字段就认为成功
        return any(field in response_json for field in result_fields)

    def _extract_error_message(self, response_json):
        """从响应中提取错误信息"""
        if 'error' in response_json:
            return response_json['error']
        return "Unknown error - no results found"

    # ----------------------------
    # 新增：按时间压测（参考 Thordata_serp.py）
    # ----------------------------

    def run_concurrent_test(self, engine, duration_seconds, concurrency, query=None):
        """
        运行并发性能测试（按时间）

        Args:
            engine: 搜索引擎名称
            duration_seconds: 运行时长（秒）
            concurrency: 并发数
            query: 搜索关键词（可选，默认按引擎关键词池）

        Returns:
            tuple: (所有请求结果, 总耗时, 总请求数)
        """
        results = []
        start_monotonic = time.perf_counter()
        end_time = start_monotonic + duration_seconds

        keyword_source = self.engine_keyword_map.get(engine, self.keyword_pool) if query is None else None
        special_engines = {"google_lens", "google_flights", "google_trends", "google_hotels", "google_maps"}
        use_round_robin = query is None and engine not in special_engines
        request_counter = itertools.count() if use_round_robin else None

        def next_query():
            if query is not None:
                return query
            if engine in special_engines:
                return random.choice(keyword_source)
            idx = next(request_counter)
            return keyword_source[idx % len(keyword_source)]

        print(f"\n开始测试引擎: {engine}")
        print(f"  运行时间: {duration_seconds}秒")
        print(f"  并发数: {concurrency}")
        print(f"  缓存: 禁用 (no_cache=true)")
        print("-" * 80)

        total_start_time = start_monotonic

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(self._run_worker_until, engine, next_query, end_time)
                for _ in range(concurrency)
            ]

            for idx, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    worker_results = future.result()
                    results.extend(worker_results)
                except Exception as e:
                    print(f"  线程 {idx} 异常: {str(e)}")

        total_end_time = time.perf_counter()
        total_duration = round(total_end_time - total_start_time, 3)
        total_requests = len(results)

        print(f"\n并发测试完成，总耗时: {total_duration}秒，总请求数: {total_requests}")

        return results, total_duration, total_requests

    def _run_worker_until(self, engine, next_query_fn, end_time):
        """
        Worker 线程：在截止时间前持续发送请求，不再新增超时请求
        """
        worker_results = []
        current_time = time.perf_counter()
        if current_time >= end_time:
            return worker_results

        while True:
            current_time = time.perf_counter()
            if current_time >= end_time:
                break
            result = self.make_request(engine, next_query_fn())
            worker_results.append(result)
            current_time = time.perf_counter()
            if current_time >= end_time:
                break
        return worker_results

    def run_all_engines_test(self, engines, duration_seconds, concurrency, query=None):
        """
        测试多个引擎的性能（按时间）
        """
        all_results = {}
        all_statistics = []

        print("=" * 80)
        print("开始批量引擎性能测试")
        print("=" * 80)

        for engine in engines:
            try:
                results, total_duration, total_requests = self.run_concurrent_test(
                    engine, duration_seconds, concurrency, query=query
                )

                all_results[engine] = results

                stats = self._calculate_statistics(
                    'SerpAPI', engine, results, total_requests,
                    concurrency, total_duration
                )
                all_statistics.append(stats)

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
                              concurrency, total_duration):
        """
        计算统计数据（补齐 Thordata_serp.py 的字段：错误率、P95、P99）
        """
        successful_results = [r for r in results if r['success']]
        success_count = len(successful_results)
        success_rate = round(success_count / total_requests * 100, 2) if total_requests > 0 else 0

        avg_response_time = 0
        if successful_results:
            total_time = sum(r['response_time'] for r in successful_results if r['response_time'] is not None)
            avg_response_time = round(total_time / len(successful_results), 3) if successful_results else 0

        p50_latency = p75_latency = p90_latency = p95_latency = p99_latency = 0
        if successful_results:
            response_times = sorted([r['response_time'] for r in successful_results if r['response_time'] is not None])
            if response_times:
                def get_percentile_value(times, percentile):
                    index = math.ceil(len(times) * percentile) - 1
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

        # QPS/速率：按 “实际总请求数 / 实际总耗时” 计算（用于找最佳QPS）
        request_rate = round(total_requests / total_duration, 3) if total_duration > 0 else 0

        error_rate = self._compute_error_rate(success_count, total_requests)

        avg_response_size = 0
        if successful_results:
            total_size = sum(r['response_size'] for r in successful_results if r['response_size'] is not None)
            avg_response_size = round(total_size / len(successful_results), 3) if successful_results else 0

        stats = {
            '产品类别': product,
            '引擎': engine,
            '请求总数': total_requests,
            '并发数': concurrency,
            '请求速率(req/s)': request_rate,
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

    def _save_detailed_csv(self, engine, results, concurrency):
        """
        保存详细的请求记录到CSV（文件名带并发值，方便区分）
        """
        filename = f"serpapi_{engine}_c{concurrency}_detailed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        fieldnames = [
            'timestamp', 'product', 'engine', 'query', 'status_code',
            'response_time', 'response_size', 'success', 'error', 'response_excerpt'
        ]

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
            csvfile.flush()
            os.fsync(csvfile.fileno())

        print(f"  详细记录已保存到: {filename}")

    def save_summary_statistics(self, statistics, filename='serpapi_summary_statistics.csv'):
        """
        保存汇总统计表（补齐 Thordata_serp.py 字段）
        """
        if not statistics:
            print("没有统计数据可保存")
            return

        fieldnames = [
            '产品类别', '引擎', '请求总数', '并发数', '请求速率(req/s)',
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

        self._print_statistics_table(statistics)

    def _print_statistics_table(self, statistics):
        """在控制台打印统计表（对齐 Thordata_serp.py）"""
        print("\n汇总统计表:")
        header = f"{'引擎':<20} {'请求数':>8} {'并发':>6} {'速率(req/s)':>12} " \
                 f"{'成功':>8} {'成功率':>8} {'错误率':>8} {'平均响应(s)':>12} " \
                 f"{'P50延迟(s)':>11} {'P75延迟(s)':>11} {'P90延迟(s)':>11} {'P95延迟(s)':>11} {'P99延迟(s)':>11} " \
                 f"{'完成时间(s)':>12} {'响应大小(KB)':>14}"
        table_width = len(header)
        print("-" * table_width)
        print(header)
        print("-" * table_width)

        for stat in statistics:
            row = f"{stat['引擎']:<20} {stat['请求总数']:>8} {stat['并发数']:>6} " \
                  f"{stat['请求速率(req/s)']:>12} {stat['成功次数']:>8} " \
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
  # 测试单个引擎（按时间）
  python SerpApi_test.py -k YOUR_API_KEY -e google -t 60 -c 5

  # 测试多个引擎（按时间）
  python SerpApi_test.py -k YOUR_API_KEY -e google bing yandex -t 120 -c 10

  # 测试所有引擎（按时间）
  python SerpApi_test.py -k YOUR_API_KEY --all-engines -t 60 -c 5

  # 连续并发压测（例如先20并发再50并发）
  python SerpApi_test.py -k YOUR_API_KEY -e google -t 60 --concurrency-steps 20 50

  # 启用详细CSV记录（详细CSV文件名带并发值）
  python SerpApi_test.py -k YOUR_API_KEY -e google -t 60 -c 5 --save-details
        """
    )

    parser.add_argument('-k', '--api-key', type=str, help='SerpAPI认证密钥')
    parser.add_argument('-e', '--engines', type=str, nargs='+', help='要测试的搜索引擎列表')
    parser.add_argument('--all-engines', action='store_true', help='测试所有支持的引擎')

    # 从“次数”改为“时间”
    parser.add_argument('-t', '--duration', type=int, default=60,
                        help='每个引擎的运行时间(秒) (默认: 60)')

    parser.add_argument('-c', '--concurrency', type=int, default=5, help='并发数 (默认: 5)')
    parser.add_argument('--concurrency-steps', type=int, nargs='+',
                        help='连续执行的并发列表，例如: --concurrency-steps 20 50')

    parser.add_argument('-q', '--query', type=str, help='搜索关键词 (默认: 随机)')

    parser.add_argument('--save-details', action='store_true', help='保存每个请求的详细CSV记录')
    parser.add_argument('-o', '--output', type=str, default='serpapi_summary_statistics.csv',
                        help='汇总统计表输出文件名')
    parser.add_argument('--list-engines', action='store_true', help='列出所有支持的引擎')

    args = parser.parse_args()

    if args.list_engines:
        print("支持的搜索引擎:")
        for i, engine in enumerate(SerpAPITester.SUPPORTED_ENGINES, 1):
            print(f"  {i:2d}. {engine}")
        return

    if not args.api_key:
        print("错误: 请使用 -k 或 --api-key 指定API密钥")
        return

    if args.all_engines:
        engines = SerpAPITester.SUPPORTED_ENGINES
        print(f"将测试所有 {len(engines)} 个引擎")
    elif args.engines:
        engines = args.engines
        invalid_engines = [e for e in engines if e not in SerpAPITester.SUPPORTED_ENGINES]
        if invalid_engines:
            print(f"警告: 以下引擎不在支持列表中: {', '.join(invalid_engines)}")
            print("使用 --list-engines 查看支持的引擎列表")
    else:
        print("错误: 请使用 -e 指定引擎或使用 --all-engines 测试所有引擎")
        print("使用 --list-engines 查看支持的引擎列表")
        return

    tester = SerpAPITester(args.api_key, save_details=args.save_details)

    # 运行测试：支持连续并发配置（参考 Thordata）
    concurrency_list = args.concurrency_steps if args.concurrency_steps else [args.concurrency]
    all_results = {}
    all_statistics = []

    for conc in concurrency_list:
        print(f"\n==== 开始并发 {conc} 的测试 ====")
        results, statistics = tester.run_all_engines_test(
            engines, args.duration, conc, query=args.query
        )
        all_results.update(results)
        all_statistics.extend(statistics)

    tester.save_summary_statistics(all_statistics, args.output)

    print("\n测试完成!")


if __name__ == "__main__":
    main()