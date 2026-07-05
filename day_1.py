import asyncio
import aiohttp
import aiofiles # добавил импорт, но по требованиям не понял, где неоходимо читать или записывать файлы асинхронно
import time
from urllib.parse import urlparse
from day_3 import CrawlerQueue, SemaphoreManager
class AsyncCrawler:
    
    def __init__(
        self, 
        max_concurrent: int = 10,
        max_depth: int = 2,
        max_per_domain: int = 2
        ):
        
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.max_per_domain = max_per_domain

        timeout = aiohttp.ClientTimeout (
            total = 10,
            connect = 5,
            sock_read = 6
        )

        connector = aiohttp.TCPConnector(
            limit=max_concurrent,
            limit_per_host=max_per_domain
        )
        
        self.session = aiohttp.ClientSession (
            timeout=timeout,
            connector=connector)

        self.queue = CrawlerQueue()
        
        self.semaphore_manager = SemaphoreManager(
            max_concurrent=max_concurrent,
            max_per_domain=max_per_domain
        )

        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.last_errors = {}

        self.same_domain_only = True
        self.exclude_patterns = []
        self.include_patterns = []

    async def fetch_url (self, url: str) -> str:
        
        await self.semaphore_manager.acquire(url)

        try:
            print(f"Начинается загрузка {url}")

            async with self.session.get(url) as response:
                response.raise_for_status()

                html = await response.text()
                
                print(f"Успешно загружено {url}") 
                return html
            
        except aiohttp.ClientResponseError as e:
            error = f"HTTP ошибка. Статус: {e.status}"
            self.last_errors[url] = error
            print(f"Ошибка: {url}. Статус: {e.status}")
            return ""
        
        except asyncio.TimeoutError:
            error = "Таймаут запроса"
            self.last_errors[url] = error
            print(f"Таймаут: {url}")
            return ""

        except aiohttp.ClientError as e:
            error = f"Сетевая ошибка: {e}"
            self.last_errors[url] = error
            print(f"Сетевая ошибка: {url} | {e}")
            return ""
        
        finally:
            self.semaphore_manager.release(url)
        
    async def fetch_and_parse(self, url: str) -> dict:
        
        html = await self.fetch_url (url)

        if not html:
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
                "headings": [],
                "tables": [],
                "lists": []
            }

        parsed_data = await self.parser.parse_html(html, url)
        
        return parsed_data
    
    async def fetch_urls (self, urls: list [str]) -> dict[str, str]:
        
        tasks = []

        for url in urls:
            task = asyncio.create_task (self.fetch_url(url))
            tasks.append (task)

        pages = await asyncio.gather (*tasks)

        results = {}

        for url, html in zip(urls, pages):
            results[url] = html

        return results
    
    async def crawl(self, start_urls: list[str], max_pages: int = 100):
    
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.queue = CrawlerQueue()

        start_time = time.perf_counter()

        allowed_domains = set()

        for url in start_urls:
            
            parsed_url = urlparse(url)

            if parsed_url.netloc:
                allowed_domains.add(parsed_url.netloc)

            added = self.queue.add_url(url, priority=10)

            if added:
                self.queue.set_depth(url, 0)

        while len(self.processed_urls) < max_pages:
            
            url = await self.queue.get_next()

            if url is None:
                break

            if url in self.visited_urls:
                continue

            depth = self.queue.get_depth(url)

            if depth > self.max_depth:
                continue

            parsed_url = urlparse(url)

            if self.same_domain_only:
                if parsed_url.netloc not in allowed_domains:
                    continue

            if self.exclude_patterns:
                excluded = False

                for pattern in self.exclude_patterns:
                    if pattern in url:
                        excluded = True
                        break

                if excluded:
                    continue

            if self.include_patterns:
                included = False

                for pattern in self.include_patterns:
                    if pattern in url:
                        included = True
                        break

                if not included:
                    continue

            self.visited_urls.add(url)

            parsed_data = await self.fetch_and_parse(url)

            if not parsed_data["text"] and not parsed_data["links"]:
                error = "Ошибка загрузки или пустая страница"

                self.failed_urls[url] = error
                self.queue.mark_failed(url, error)

            else:
                
                self.processed_urls[url] = parsed_data
                self.queue.mark_processed(url)

                if depth < self.max_depth:
                    links = parsed_data["links"]

                    for link in links:
                        if link in self.visited_urls:
                            continue

                        link_depth = depth + 1

                        if link_depth > self.max_depth:
                            continue

                        added = self.queue.add_url(link, priority=0)

                        if added:
                            self.queue.set_depth(link, link_depth)

            elapsed = time.perf_counter() - start_time

            if elapsed > 0:
                speed = len(self.processed_urls) / elapsed
            else:
                speed = 0

            stats = self.queue.get_stats()

            print(
                f"\nПрогресс:"
                f"\nОбработано страниц: {len(self.processed_urls)}"
                f"\nВ очереди: {stats['queued']}"
                f"\nОшибок: {len(self.failed_urls)}"
                f"\nСкорость: {speed:.2f} страниц/сек"
            )

        return self.processed_urls

    async def close(self):
        
        await self.session.close()

if __name__ == "__main__":
    
    async def main(): 

    #   валидные и ошибочные урлы 

        urls = [
            "https://example.com",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/2",
            "https://www.python.org",
            "https://jsonplaceholder.typicode.com/posts/1",
            "https://httpbin.org/status/404",
            "https://httpbin.org/status/500",
            "https://httpbin.org/delay/15"
            ]
        
        # урлы с одинаковыми ключами для последовательной загрузки

        speed_urls = [
            "https://httpbin.org/delay/1?i=1",
            "https://httpbin.org/delay/1?i=2",
            "https://httpbin.org/delay/1?i=3",
            "https://httpbin.org/delay/1?i=4",
            "https://httpbin.org/delay/1?i=5",
            "https://httpbin.org/delay/1?i=6",
            "https://httpbin.org/delay/1?i=7",
            "https://httpbin.org/delay/1?i=8",
                ]

        crawler = AsyncCrawler(max_concurrent=5)

        start_time = time.perf_counter()
        
        try:
            results = await crawler.fetch_urls(urls)

            end_time = time.perf_counter()
            total_time = end_time - start_time

            print("\nРезультат:")

            for url, html in results.items():
                if html:
                    print(f"{url} - успешно загружено")
                else:
                    print(f"{url} - ошибка загрузки")

            print(f"\nОбщее время выполнения: {round(total_time, 2)} сек.")
            print(f"Всего URL: {len(urls)}")
            print(f"Успешно загружено: {sum(1 for html in results.values() if html)}")
            print(f"Ошибок: {sum(1 for html in results.values() if not html)}")
        
        #   проверка времени выполнения последовательной загрузки   

            print (f"\nПоследовательная загрузка")
            sequential_start = time.perf_counter()

            sequential_results = {}
            
            for url in speed_urls:
                html = await crawler.fetch_url(url)
                sequential_results[url] = html

            sequential_time = time.perf_counter() - sequential_start
            print(f"\nОбщее время выполнения последовательной загрузки: {round(sequential_time, 2)} сек.")
            print(f"Всего URL: {len(speed_urls)}")
        
        
        finally:
            await crawler.close()

    asyncio.run(main())