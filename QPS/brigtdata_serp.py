#!/usr/bin/env python3
"""
Bright Data SERP Performance Test Script

Adapted from the Thordata SerpAPI tester, this script benchmarks Bright Data's
SERP proxy across multiple Google-facing engines (Search, Maps, Trends). It
constructs per-engine request payloads, executes concurrent runs, and records
latency, success rate, and response size statistics. Both raw (HTML) and JSON
responses are supported.
"""

import argparse
import concurrent.futures
import csv
import itertools
import json
import math
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, parse_qs, quote

import requests


@dataclass
class EngineConfig:
    """Configuration for how to build a Bright Data request for an engine."""

    name: str
    base_url: str
    required_param: str = "q"
    extra_params: Dict[str, Any] = None
    query_in_path: bool = False  # If True, append query to path instead of as parameter
    include_brd_json: bool = True  # If False, don't add brd_json parameter

    def build_url(self, query: Any, brd_json: Optional[int] = 1) -> str:
        """
        Build a properly formatted URL for the engine according to Bright Data specifications.

        This method ensures:
        - No trailing ampersands (&) in the URL
        - Proper handling of base URLs with or without existing query parameters
        - Correct parameter encoding and concatenation
        - Support for query-in-path format (e.g., maps)

        Note: If base_url contains duplicate parameter names, only the first value is preserved.
        URL fragments (parts after #) are not preserved as they are not used in SERP APIs.
        """
        params: Dict[str, Any] = {}

        # Parse the base URL to extract any existing query parameters
        parsed = urlparse(self.base_url)
        existing_params = parse_qs(parsed.query)

        # Flatten existing params (parse_qs returns lists; only first value is preserved)
        for key, values in existing_params.items():
            if values:
                params[key] = values[0]

        # Handle query_in_path format (for engines like maps)
        path_suffix = ""
        if self.query_in_path:
            if isinstance(query, dict):
                # If query is a dict, use the required_param value for path
                query_value = query.get(self.required_param, "")
                if query_value:
                    # URL encode but preserve common safe characters for readability
                    path_suffix = quote(str(query_value), safe='-_.~')
                # Add other dict items as regular params
                params.update({k: v for k, v in query.items() if k != self.required_param})
            else:
                # Simple query string goes in path
                # URL encode but preserve common safe characters for readability
                path_suffix = quote(str(query), safe='-_.~')
        else:
            # Standard parameter handling
            if isinstance(query, dict):
                params.update(query)
            else:
                params[self.required_param] = query

        # Add extra parameters only if they don't already exist (setdefault preserves existing)
        if self.extra_params:
            for key, value in self.extra_params.items():
                params.setdefault(key, value)

        # Ensure Bright Data returns JSON by default, unless the caller opts out
        # Only add if engine config allows it
        if brd_json is not None and self.include_brd_json:
            params["brd_json"] = brd_json

        # Encode parameters, filtering out None values
        encoded = urlencode({k: v for k, v in params.items() if v is not None})

        # Reconstruct URL with clean base (without query string, fragment, or trailing ?)
        clean_base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        # Add path suffix if needed (e.g., for maps: /search/hotels/)
        if path_suffix:
            # Ensure no double slashes by removing trailing slash from base
            clean_base = clean_base.rstrip('/') + '/' + path_suffix.lstrip('/') + '/'

        return f"{clean_base}?{encoded}" if encoded else clean_base


class BrightDataTester:
    """Bright Data SERP 性能测试类"""

    API_URL = "https://api.brightdata.com/request"

    SUPPORTED_ENGINES: Dict[str, EngineConfig] = {
        "search": EngineConfig(name="search", base_url="https://www.google.com/search"),
        # Google Maps place/POI lookups. URL format: /maps/search/{query}/
        "maps": EngineConfig(
            name="maps",
            base_url="https://www.google.com/maps/search",
            required_param="q",
            query_in_path=True,
        ),
        # Google Trends keyword popularity. Requires brd_trends parameter for widget data.
        "trends": EngineConfig(
            name="trends",
            base_url="https://trends.google.com/trends/explore",
            extra_params={"brd_trends": "timeseries,geo_map"},
        ),
        # Google local reviews surface (Local Pack). tbm=lcl switches the vertical to reviews.
        "reviews": EngineConfig(
            name="reviews",
            base_url="https://www.google.com/search",
            extra_params={"tbm": "lcl"},
        ),
        # Google Lens reverse image search via URL input. Minimal parameters only.
        "lens": EngineConfig(
            name="lens",
            base_url="https://lens.google.com/uploadbyurl",
            required_param="url",
            include_brd_json=False,
        ),
        # Google Hotels vertical. q takes the destination/city or hotel name.
        "hotels": EngineConfig(
            name="hotels",
            base_url="https://www.google.com/travel/hotels",
        ),
        # Google Flights vertical. q expects origin/destination/free text like "SFO to JFK".
        "flights": EngineConfig(
            name="flights",
            base_url="https://www.google.com/travel/flights",
        ),
        # Bing search engine. Uses q parameter for queries.
        "bing": EngineConfig(
            name="bing",
            base_url="https://www.bing.com/search",
        ),
        # Yandex search engine. Uses text parameter for queries.
        "yandex": EngineConfig(
            name="yandex",
            base_url="https://www.yandex.com/search/",
            required_param="text",
        ),
        # DuckDuckGo search engine. Uses q parameter for queries.
        "duckduckgo": EngineConfig(
            name="duckduckgo",
            base_url="https://duckduckgo.com/",
        ),
    }

    ENGINE_SAMPLE_QUERIES: Dict[str, List[Any]] = {
        "maps": [
            "pizza",
            "coffee",
            "restaurant",
            "hotel",
            "gym",
            "theater",
            "museums",
#            "transit",
            "pharmacy",
        ],
        "trends": [
            {"q": "ai news", "geo": "US", "date": "now 7-d"},
            {"q": "bitcoin", "geo": "US", "date": "today 2-m"},
            {"q": "nba", "geo": "US", "date": "now 1-d"},
            {"q": "iphone17", "geo": "US", "date": "now 1-d"},
            {"q": "PS5", "geo": "US", "date": "now 7-d"},
            {"q": "switch2", "geo": "US", "date": "now 7-d"},
        ],
        "reviews": [
            "best sushi in nyc",
            "coffee shop san francisco",
            "bakery london",
            "dentist seattle",
            "hotel shenzhen",
        ],
        "lens": [
            "https://i.imgur.com/HBrB8p0.png",
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
        "hotels": [
            {"q": "Bali Resorts", "checkin": "2025-12-17", "checkout": "2025-12-18", "adults": 2},
            {"q": "Tokyo luxury hotels", "checkin": "2025-12-15", "checkout": "2025-12-20", "adults": 1},
            {"q": "New York boutique hotels", "checkin": "2025-12-22", "checkout": "2025-12-26", "adults": 1},
            {"q": "Paris family hotels", "checkin": "2026-01-05", "checkout": "2026-01-09", "adults": 1},
            {"q": "Sydney beach resorts", "checkin": "2026-02-10", "checkout": "2026-02-15", "adults": 2},
        ],
        "flights": [
            {"q": "SFO to JFK", "src": "searchbox"},
            {"q": "LAX to NRT", "src": "searchbox"},
            {"q": "PEK to PVG", "src": "searchbox"},
            {"q": "CDG to LHR", "src": "searchbox"},
            {"q": "BOS to MIA", "src": "searchbox"},
        ],
    }

    KEYWORD_POOL = [
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

    def __init__(
            self,
            api_token: str,
            zone: str,
            response_format: str = "raw",
            save_details: bool = False,
            brd_json: Optional[int] = 1,
    ):
        self.api_token = api_token
        self.zone = zone
        self.response_format = response_format
        self.save_details = save_details
        self.brd_json = brd_json
        self.session = requests.Session()  # Reuse connection pool

    def _build_payload(self, engine: str, query: Any) -> Dict[str, Any]:
        if engine not in self.SUPPORTED_ENGINES:
            raise ValueError(f"不支持的引擎: {engine}")

        config = self.SUPPORTED_ENGINES[engine]
        url = config.build_url(query, brd_json=self.brd_json)

        payload = {
            "zone": self.zone,
            "url": url,
            "format": self.response_format,
        }
        return payload

    def make_request(self, engine: str, query: Any) -> Dict[str, Any]:
        result = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "engine": engine,
            "query": json.dumps(query, ensure_ascii=False) if isinstance(query, dict) else str(query),
            "status_code": None,
            "response_time": None,
            "response_size": None,
            "success": False,
            "error": "",
            "response_excerpt": "",
            "product": "Brightdata",
        }

        payload = self._build_payload(engine, query)
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

        start_time = time.perf_counter()
        try:
            response = self.session.post(self.API_URL, json=payload, headers=headers, timeout=60)
            duration = round(time.perf_counter() - start_time, 3)

            result["status_code"] = response.status_code
            result["response_time"] = duration
            result["response_size"] = round(len(response.content) / 1024, 3)
            parsed_json = self._try_parse_json(response)
            result["response_excerpt"] = self._extract_excerpt(parsed_json, response)

            success, error_message = self._evaluate_response(response, parsed_json)
            result["success"] = success
            result["error"] = error_message
            return result
        except Exception as exc:
            duration = round(time.perf_counter() - start_time, 3)
            result["response_time"] = duration
            result["success"] = False
            result["error"] = f"Request error: {exc}"
            return result

    def _evaluate_response(
            self, response: requests.Response, parsed_json: Optional[Dict[str, Any]]
    ) -> Tuple[bool, str]:
        if response.status_code != 200:
            return False, f"HTTP {response.status_code}"

        if not response.content:
            return False, "Empty response"

        if parsed_json is not None:
            if isinstance(parsed_json, dict):
                error_message = self._extract_error_from_payload(parsed_json)
                if error_message:
                    return False, error_message

                proxy_status = parsed_json.get("status_code")
                if proxy_status and proxy_status != 200:
                    return False, f"Proxy status {proxy_status}"

        return True, ""

    def _try_parse_json(self, response: requests.Response) -> Optional[Dict[str, Any]]:
        # When the caller requested JSON, attempt to parse even if the content type is missing
        # or incorrect, to better surface Bright Data payload errors/excerpts.
        if self.response_format != "json":
            content_type = response.headers.get("Content-Type", "").lower()
            if "json" not in content_type and not response.text.strip().startswith("{"):
                return None

        try:
            parsed = response.json()
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    @staticmethod
    def _extract_error_from_payload(payload: Dict[str, Any]) -> str:
        if payload.get("error"):
            detail_messages: List[str] = []
            details = payload.get("details")
            if isinstance(details, list):
                for item in details:
                    message = item.get("message") if isinstance(item, dict) else None
                    if message:
                        detail_messages.append(message)
            detail_suffix = f": {'; '.join(detail_messages)}" if detail_messages else ""
            error_code = f" ({payload.get('error_code')})" if payload.get("error_code") else ""
            return f"{payload.get('error')}{error_code}{detail_suffix}"
        return ""

    def _extract_excerpt(self, parsed_json: Optional[Dict[str, Any]], response: requests.Response) -> str:
        if parsed_json:
            # For JSON responses, prioritize a JSON snippet so the CSV clearly shows
            # the structured payload instead of embedded HTML.
            if self.response_format == "json":
                return json.dumps(parsed_json, ensure_ascii=False)[:1000]

            if "body" in parsed_json and isinstance(parsed_json.get("body"), str):
                return parsed_json.get("body", "")[:1000]

            if "error" in parsed_json:
                return json.dumps(parsed_json, ensure_ascii=False)[:1000]

        return response.text[:1000]

    def _get_next_query_fn(self, engine: str, explicit_query: Optional[str]):
        """
        Create a query generator function based on engine and explicit_query.
        
        For engines with special format requirements (lens, trends, hotels, flights, maps with dict):
        - Use random.choice from engine-specific pool
        
        For regular engines (search, reviews, bing, yandex, duckduckgo):
        - If explicit_query provided: always return that
        - Otherwise: use round-robin from keyword pool
        """
        if explicit_query:
            # User specified query, always use it
            return lambda: explicit_query
        
        # Engines that need dict/special format
        engine_queries = self.ENGINE_SAMPLE_QUERIES.get(engine)
        if engine_queries:
            # Random choice for stability with structured queries
            return lambda: random.choice(engine_queries)
        
        # Regular engines: use itertools.cycle for thread-safe round-robin
        pool = self.KEYWORD_POOL
        cycle_iter = itertools.cycle(pool)
        lock = threading.Lock()
        
        def round_robin_query():
            with lock:
                return next(cycle_iter)
        
        return round_robin_query

    def _run_worker_until(self, engine: str, next_query_fn, end_time: float, concurrency: int) -> List[Dict[str, Any]]:
        """
        Worker function that runs requests until end_time is reached.
        Returns list of result dictionaries.
        """
        results = []
        while time.perf_counter() < end_time:
            query = next_query_fn()
            result = self.make_request(engine, query)
            result["concurrency"] = concurrency
            results.append(result)
        return results

    def run_concurrent_test(
        self, engine: str, duration_seconds: int, concurrency: int, explicit_query: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Run duration-based concurrent test for a single engine.
        
        Args:
            engine: Engine name to test
            duration_seconds: How long to run the test (in seconds)
            concurrency: Number of concurrent workers
            explicit_query: Optional explicit query to use for all requests
            
        Returns:
            Tuple of (results list, statistics dict)
        """
        end_time = time.perf_counter() + duration_seconds
        next_query_fn = self._get_next_query_fn(engine, explicit_query)
        
        results: List[Dict[str, Any]] = []
        start = time.perf_counter()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            # Submit workers
            futures = [
                executor.submit(self._run_worker_until, engine, next_query_fn, end_time, concurrency)
                for _ in range(concurrency)
            ]
            
            # Collect results from all workers
            for future in concurrent.futures.as_completed(futures):
                worker_results = future.result()
                results.extend(worker_results)
        
        actual_duration = round(time.perf_counter() - start, 3)
        total_requests = len(results)
        
        stats = self._calculate_statistics(engine, total_requests, concurrency, actual_duration, results)
        
        if self.save_details:
            self._save_detailed_csv(engine, concurrency, results)
        
        return results, stats

    def run_all_engines_test(self, engines: Iterable[str], duration_seconds: int, concurrency: int,
                             explicit_query: Optional[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        all_results: List[Dict[str, Any]] = []
        all_stats: List[Dict[str, Any]] = []

        for engine in engines:
            print(f"\n=== 开始测试引擎: {engine} ===")
            results, stats = self.run_concurrent_test(engine, duration_seconds, concurrency, explicit_query)
            all_results.extend(results)
            all_stats.append(stats)
            print(f"=== 引擎 {engine} 测试完成 ===")

        return all_results, all_stats

    def _calculate_statistics(self, engine: str, total_requests: int, concurrency: int, duration: float,
                              results: List[Dict[str, Any]]) -> Dict[str, Any]:
        successes = [r for r in results if r.get("success")]
        response_times_success = [r.get("response_time") for r in successes if r.get("response_time") is not None]

        success_count = len(successes)
        success_rate = round((success_count / total_requests) * 100, 2) if total_requests else 0
        error_rate = round(((total_requests - success_count) / total_requests) * 100, 2) if total_requests else 0
        avg_response_time = round(sum(response_times_success) / len(response_times_success),
                                  3) if response_times_success else 0

        def percentile(values: List[float], pct: float) -> float:
            if not values:
                return 0
            values_sorted = sorted(values)
            rank = max(1, math.ceil(pct * len(values_sorted))) - 1
            return round(values_sorted[rank], 3)

        stats = {
            "产品类别": "Brightdata",
            "引擎": engine,
            "请求总数": total_requests,
            "并发数": concurrency,
            "成功次数": success_count,
            "成功率(%)": success_rate,
            "错误率(%)": error_rate,
            "请求速率(req/s)": round(total_requests / duration, 3) if duration > 0 else 0,
            "成功平均响应时间(s)": avg_response_time,
            "P50延迟(s)": percentile(response_times_success, 0.5),
            "P75延迟(s)": percentile(response_times_success, 0.75),
            "P90延迟(s)": percentile(response_times_success, 0.9),
            "P95延迟(s)": percentile(response_times_success, 0.95),
            "P99延迟(s)": percentile(response_times_success, 0.99),
            "并发完成时间(s)": duration,
            "成功平均响应大小(KB)": round(
                sum(r.get("response_size", 0) for r in successes) / len(successes), 3
            ) if successes else 0,
        }

        self._print_statistics_table([stats])
        return stats

    def _save_detailed_csv(self, engine: str, concurrency: int, results: List[Dict[str, Any]]) -> None:
        filename = f"brightdata_{engine}_c{concurrency}_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        fieldnames = [
            "timestamp",
            "engine",
            "concurrency",
            "query",
            "status_code",
            "response_time",
            "response_size",
            "success",
            "error",
            "response_excerpt",
            "product",
        ]

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        print(f"详细记录已保存到: {filename}")

    def save_summary_statistics(self, statistics: List[Dict[str, Any]], filename: str) -> None:
        if not statistics:
            print("没有统计数据可保存")
            return

        fieldnames = [
            "产品类别",
            "引擎",
            "请求总数",
            "并发数",
            "请求速率(req/s)",
            "成功次数",
            "成功率(%)",
            "错误率(%)",
            "成功平均响应时间(s)",
            "P50延迟(s)",
            "P75延迟(s)",
            "P90延迟(s)",
            "P95延迟(s)",
            "P99延迟(s)",
            "并发完成时间(s)",
            "成功平均响应大小(KB)",
        ]

        with open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(statistics)

        print(f"\n汇总统计表已保存到: {filename}")
        self._print_statistics_table(statistics)

    def _print_statistics_table(self, statistics: List[Dict[str, Any]]) -> None:
        print("\n汇总统计表:")
        print("-" * 180)
        header = (
            f"{'产品':<10} {'引擎':<12} {'请求数':>8} {'并发':>6} {'速率(req/s)':>12} "
            f"{'成功':>8} {'成功率':>8} {'错误率':>8} {'平均响应(s)':>12} "
            f"{'P50':>8} {'P75':>8} {'P90':>8} {'P95':>8} {'P99':>8} "
            f"{'完成时间(s)':>12} {'响应大小(KB)':>14}"
        )
        print(header)
        print("-" * 180)

        for stat in statistics:
            row = (
                f"{stat['产品类别']:<10} {stat['引擎']:<12} {stat['请求总数']:>8} {stat['并发数']:>6} "
                f"{stat['请求速率(req/s)']:>12} {stat['成功次数']:>8} "
                f"{stat['成功率(%)']:>8}% {stat['错误率(%)']:>8}% {stat['成功平均响应时间(s)']:>12} "
                f"{stat['P50延迟(s)']:>8} {stat['P75延迟(s)']:>8} {stat['P90延迟(s)']:>8} "
                f"{stat['P95延迟(s)']:>8} {stat['P99延迟(s)']:>8} "
                f"{stat['并发完成时间(s)']:>12} {stat['成功平均响应大小(KB)']:>14}"
            )
            print(row)

        print("-" * 180)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bright Data SERP 性能测试脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 测试单个引擎 60 秒 (默认随机关键词)
  python brigtdata_serp.py -t YOUR_TOKEN -z serp_api1 -e search -d 60 -c 5
 
  # 指定查询关键词并测试多个引擎
  python brigtdata_serp.py -t YOUR_TOKEN -z serp_api1 -e search -d 30 -c 3 --format json --brd-json 1 --save-details

  # 为同一批引擎依次运行多个并发配置
  python brigtdata_serp.py -t YOUR_TOKEN -z serp_api1 -e search maps -d 30 -c 1 5 10

  # 使用 JSON 响应格式并保存详细记录
  python brigtdata_serp.py -t YOUR_TOKEN -z serp_api1 -e search --format json --save-details
        """,
    )

    parser.add_argument("-t", "--api-token", help="Bright Data API Token")
    parser.add_argument("-z", "--zone", default="serp_api1", help="Bright Data zone 名称")
    parser.add_argument("-e", "--engines", nargs="+", help="要测试的引擎列表")
    parser.add_argument("--all-engines", action="store_true", help="测试所有支持的引擎")
    parser.add_argument("-d", "--duration", type=int, default=60, help="每个引擎测试时长（秒）")
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        nargs="+",
        default=[3],
        help="并发数，可指定多个值依次运行",
    )
    parser.add_argument("-q", "--query", help="指定查询关键词 (默认随机)")
    parser.add_argument("--format", choices=["raw", "json"], default="raw", help="Bright Data 响应格式")
    parser.add_argument("--save-details", action="store_true", help="保存每个请求的详细 CSV 记录")
    parser.add_argument("-o", "--output", default="brightdata_summary_statistics.csv", help="汇总统计输出文件名")
    parser.add_argument("--list-engines", action="store_true", help="列出所有支持的引擎")
    parser.add_argument(
        "--brd-json",
        type=int,
        choices=[0, 1],
        default=1,
        help="是否在 URL 中附加 brd_json 参数 (默认 1；设置为 0 时不追加)",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.list_engines:
        print("支持的引擎:")
        for i, engine in enumerate(BrightDataTester.SUPPORTED_ENGINES.keys(), 1):
            print(f"  {i:2d}. {engine}")
        return

    if not args.api_token:
        print("错误: 需要提供 API Token (-t/--api-token)")
        return

    if not args.all_engines and not args.engines:
        print("错误: 请使用 -e 指定引擎或使用 --all-engines 测试所有引擎")
        return

    engines = list(BrightDataTester.SUPPORTED_ENGINES.keys()) if args.all_engines else args.engines
    concurrency_values = args.concurrency

    tester = BrightDataTester(
        api_token=args.api_token,
        zone=args.zone,
        response_format=args.format,
        save_details=args.save_details,
        brd_json=args.brd_json if args.brd_json != 0 else None,
    )

    all_statistics: List[Dict[str, Any]] = []
    for concurrency in concurrency_values:
        print(f"\n>>> 并发数 {concurrency} 测试开始")
        _, statistics = tester.run_all_engines_test(engines, args.duration, concurrency, args.query)
        all_statistics.extend(statistics)
    tester.save_summary_statistics(all_statistics, args.output)


if __name__ == "__main__":
    main()