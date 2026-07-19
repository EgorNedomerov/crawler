import asyncio
import json
import time
import tracemalloc
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from day_7 import AdvancedCrawler, setup_logging

HOST = "127.0.0.1"
PORT = 8090
BASE_URL = f"http://{HOST}:{PORT}"

class TestPageHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path.startswith("/page"):
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Test page {self.path}</title>
            </head>
            <body>
                <h1>Test page {self.path}</h1>
                <p>This is a test page for performance testing.</p>
                <a href="{BASE_URL}/page-next">Next page</a>
            </body>
            </html>
            """

            encoded = html.encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return

def start_test_server():

    server = ThreadingHTTPServer((HOST, PORT), TestPageHandler)

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True
    )

    thread.start()

    return server

def build_test_urls(count: int) -> list[str]:

    urls = []

    for index in range(count):
        urls.append(f"{BASE_URL}/page-{index}")

    return urls

def fetch_sync(url: str, timeout: int = 10) -> str:

    request = Request(
        url,
        headers={
            "User-Agent": "MyCrawler/1.0"
        }
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", errors="ignore")

    except HTTPError:
        return ""

    except URLError:
        return ""

    except Exception:
        return ""

def crawl_sync(urls: list[str]) -> dict[str, str]:

    results = {}

    for url in urls:
        results[url] = fetch_sync(url)

    return results

async def run_async_crawler(urls: list[str]) -> tuple[float, int, int]:
    
    crawler = AdvancedCrawler(
        start_urls=urls,
        max_pages=len(urls),
        max_depth=0,
        max_concurrent=50,
        max_per_domain=50,
        rate_limit=10_000.0,
        respect_robots=False,
        same_domain_only=False,
        storage=None
    )
    try:
        start_time = time.perf_counter()

        await crawler.crawl()

        elapsed = time.perf_counter() - start_time

        stats = crawler.get_stats()

        successful = stats["successful"]
        failed = stats["failed"]

        return elapsed, successful, failed

    finally:
        await crawler.close()

async def main():

    setup_logging(
        filename="performance.log",
        level="WARNING"
    )

    server = start_test_server()

    report = []

    try:
        for count in [100, 500, 1000]:
            print(f"\nТест масштабируемости: {count} страниц")

            urls = build_test_urls(count)

            tracemalloc.start()

            async_time, async_successful, async_failed = await run_async_crawler(urls)

            async_current_memory, async_peak_memory = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            tracemalloc.start()

            sync_start = time.perf_counter()
            sync_results = crawl_sync(urls)
            sync_time = time.perf_counter() - sync_start

            sync_current_memory, sync_peak_memory = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            sync_successful = sum(1 for html in sync_results.values() if html)
            sync_failed = count - sync_successful

            if async_time > 0:
                async_speed = count / async_time
            else:
                async_speed = 0

            if sync_time > 0:
                sync_speed = count / sync_time
            else:
                sync_speed = 0

            if async_time > 0:
                speedup = sync_time / async_time
            else:
                speedup = 0

            async_peak_mb = async_peak_memory / 1024 / 1024
            sync_peak_mb = sync_peak_memory / 1024 / 1024

            result = {
                "pages": count,
                "async": {
                    "time_sec": round(async_time, 2),
                    "speed_pages_sec": round(async_speed, 2),
                    "successful": async_successful,
                    "failed": async_failed,
                    "peak_memory_mb": round(async_peak_mb, 2)
                },
                "sync": {
                    "time_sec": round(sync_time, 2),
                    "speed_pages_sec": round(sync_speed, 2),
                    "successful": sync_successful,
                    "failed": sync_failed,
                    "peak_memory_mb": round(sync_peak_mb, 2)
                },
                "speedup": round(speedup, 2)
            }

            report.append(result)

            print(f"Async время: {round(async_time, 2)} сек.")
            print(f"Sync время: {round(sync_time, 2)} сек.")
            print(f"Async скорость: {round(async_speed, 2)} страниц/сек.")
            print(f"Sync скорость: {round(sync_speed, 2)} страниц/сек.")
            print(f"Ускорение async: {round(speedup, 2)}x")
            print(f"Async успешно: {async_successful}")
            print(f"Async ошибок: {async_failed}")
            print(f"Sync успешно: {sync_successful}")
            print(f"Sync ошибок: {sync_failed}")
            print(f"Async peak memory: {round(async_peak_mb, 2)} MB")
            print(f"Sync peak memory: {round(sync_peak_mb, 2)} MB")

        with open("performance_report.json", "w", encoding="utf-8") as file:
            json.dump(
                report,
                file,
                ensure_ascii=False,
                indent=4
            )

        print("\nОтчёт сохранён в performance_report.json")

    finally:
        server.shutdown()
        server.server_close()

if __name__ == "__main__":
    asyncio.run(main())