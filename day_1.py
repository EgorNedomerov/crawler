import asyncio
import aiohttp
import aiofiles # добавил импорт, но по требованиям не понял, где неоходимо читать или записывать файлы асинхронно
import time
from urllib.parse import urlparse
from day_2 import HTMLParser
from day_3 import CrawlerQueue, SemaphoreManager
import random
from day_4 import RateLimiter, RobotsParser
from day_5 import RetryStrategy, TransientError, NetworkError, PermanentError, ParseError
from datetime import datetime
import sqlite3
import logging 
logger = logging.getLogger(__name__)

class AsyncCrawler:
    
    def __init__(
        self, 
        max_concurrent: int = 10,
        max_depth: int = 2,
        max_per_domain: int = 2,
        requests_per_second: float = 1.0,
        respect_robots: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str = "MyCrawler/1.0",
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        timeout_total: float = 10,
        timeout_connect: float = 5,
        timeout_read: float = 6,
        storage = None
        ):
        
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.max_per_domain = max_per_domain

        timeout = aiohttp.ClientTimeout (
            total = timeout_total,
            connect = timeout_connect,
            sock_read = timeout_read
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
        
        self.requests_per_second = requests_per_second
        self.respect_robots = respect_robots
        self.min_delay = min_delay
        self.jitter = jitter
        self.user_agent = user_agent
        self.timeout_total = timeout_total
        self.timeout_connect = timeout_connect
        self.timeout_read = timeout_read
        self.backoff_factor = backoff_factor
        self.storage = storage
        self.saved_count = 0 
        self.storage_errors = []
        self.last_status_codes = {}
        self.last_content_types = {}
        self.storage_retry_strategy = RetryStrategy(
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            retry_on=[
                OSError,
                sqlite3.OperationalError,
                sqlite3.DatabaseError
                      ]
        )

        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=True
        )

        self.robots_parser = RobotsParser(
            session = self.session,
            rate_limiter=self.rate_limiter,
            semaphore_manager=self.semaphore_manager,
            user_agent=self.user_agent,
            timeout=timeout
        )

        self.blocked_by_robots = []
        self.request_times = []
        self.parser = HTMLParser ()
        self.retry_strategy = RetryStrategy(
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            retry_on=[TransientError, NetworkError] 
        )

    async def fetch_url (self, url: str) -> str:
        
        async def do_request():
            
            parsed_url = urlparse(url)
            domain = parsed_url.netloc

            attempt = self.retry_strategy.current_attempt

            timeout_multiplier = self.backoff_factor ** (attempt - 1)
            
            request_timeout = aiohttp.ClientTimeout(
                total= self.timeout_total * timeout_multiplier,
                connect= self.timeout_connect * timeout_multiplier,
                sock_read= self.timeout_read * timeout_multiplier
            )

            acquired = False

            try:
                if self.respect_robots:
                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

                    await self.robots_parser.fetch_robots(base_url)

                    if not self.robots_parser.can_fetch(url, self.user_agent):
                        raise PermanentError ("URL заблокирован robots.txt", url = url, status = 403)
                    
                    robots_delay = self.robots_parser.get_crawl_delay(self.user_agent)

                    if robots_delay > 0:
                        logger.info("Crawl-delay для %s: %.2f сек.", domain, robots_delay)
                        await asyncio.sleep(robots_delay)
                
                await self.semaphore_manager.acquire(url)
                acquired = True
                
                await self.rate_limiter.acquire(domain)

                delay = self.min_delay

                if self.jitter > 0:
                    delay += random.uniform(0, self.jitter)

                if delay > 0:
                    await asyncio.sleep(delay)

                logger.info("Начинается загрузка %s", url)

                headers = {
                    "User-Agent": self.user_agent
                }
            
                async with self.session.get(url, headers=headers, timeout=request_timeout) as response:

                    self.last_status_codes[url] = response.status
                    self.last_content_types[url] = response.headers.get("Content-Type", "")
                
                    if response.status in [429, 500, 502, 503, 504]:
                        raise TransientError (f"Временнная ошибка. Статус {response.status}", url = url, status = response.status) 
                    
                    if response.status in [401, 403, 404]:
                        raise PermanentError(f"Постоянная ошибка. Статус {response.status}", url = url, status = response.status)
                    
                    if response.status >= 400:
                        raise PermanentError (f"HTTP ошибка. Статус {response.status}", url = url, status = response.status)
                                              
                    html = await response.text()
                    request_end = time.perf_counter()
                    self.request_times.append(request_end)

                    logger.info("Успешно загружено %s", url) 
                    return html
            
            except aiohttp.ClientConnectionError as e:
                raise NetworkError(f"Ошибка соединения: {e}", url = url )
            
            except asyncio.TimeoutError:
                raise TransientError(f"Таймаут запроса", url = url)

            except aiohttp.ClientError as e:
                raise NetworkError(f"Сетевая ошибка: {e}", url = url)
            
            finally:
                if acquired:
                    self.semaphore_manager.release(url)

        try:
            return await self.retry_strategy.execute_with_retry(do_request)
        
        except PermanentError as e:
            self.last_errors[url] = e.message
            self.failed_urls[url] = e.message

            if e.message == "URL заблокирован robots.txt":
                self.blocked_by_robots.append(url)

            logger.error("Постоянная ошибка: %s | %s", url, e.message)
            return ""
            
        except TransientError as e:
            self.last_errors[url] = e.message
            self.failed_urls[url] = e.message
            logger.error("Временная ошибка после всех повторов: %s | %s", url, e.message)
            return ""
        
        except NetworkError as e:
            self.last_errors[url] = e.message
            self.failed_urls[url] = e.message
            logger.error("Сетевая ошибка после всех повторов: %s | %s", url, e.message)
            return ""
           
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
        
        if self.parser is None:
            raise ValueError ("Парсер не установлен")

        try:
            parsed_data = await self.parser.parse_html(html, url)

            parsed_data["crawled_at"] = datetime.now().isoformat()
            parsed_data["status_code"] = self.last_status_codes.get(url, 0)
            parsed_data["content_type"] = self.last_content_types.get(url, "")

            return parsed_data
        
        except Exception as e:
            self.last_errors[url] = f"ОШибка парсинга: {e}"
            raise ParseError(f"Ошибка парсинга: {e}", url = url)
    
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
    
    async def crawl(
            self, 
            start_urls: list[str], 
            max_pages: int = 100,
            same_domain_only: bool = True,
            exclude_patterns: list[str] | None = None,
            include_patterns: list[str] | None = None
            ):
    
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.queue = CrawlerQueue()

        self.same_domain_only = same_domain_only
        self.exclude_patterns = exclude_patterns or []
        self.include_patterns = include_patterns or []
        
        start_time = time.perf_counter()
        allowed_domains = set()

        for url in start_urls:
            
            parsed_url = urlparse(url)

            if parsed_url.netloc:
                allowed_domains.add(parsed_url.netloc)

            added = self.queue.add_url(url, priority=10)

            if added:
                self.queue.set_depth(url, 0)

        while len(self.visited_urls) < max_pages:
            
            batch = []
            
            while len(batch) < self.max_concurrent and len(self.visited_urls) < max_pages:
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
                batch.append((url,depth))

            if not batch:
                break

            tasks = []

            for url, depth in batch:
                task = asyncio.create_task(self.fetch_and_parse(url))
                tasks.append(task)
        
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for (url, depth), parsed_data in zip(batch, results):
                
                if isinstance(parsed_data, Exception):
                    error = f"Ошибка обработки страницы {parsed_data}"
                    self.failed_urls[url] = error
                    self.queue.mark_failed(url,error)
                    continue
            
                if not parsed_data["text"] and not parsed_data["links"]:
                    error = "Ошибка загрузки или пустая страница"

                    self.failed_urls[url] = error
                    self.queue.mark_failed(url, error)

                else:
                    
                    self.processed_urls[url] = parsed_data
                    self.queue.mark_processed(url)

                    await self.save_page(parsed_data)

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
            crawler_stats = self.get_stats()

            progress_percent = round((len(self.visited_urls) / max_pages) * 100, 2)

            if speed > 0:
                remaining_pages = max_pages - len(self.visited_urls)
                eta = remaining_pages / speed
            else:
                eta = 0

            semaphore_stats = self.semaphore_manager.get_semaphore_stats()
            active_tasks = semaphore_stats["active_tasks"]

            logger.info(
                "\nПрогресс: %.2f%%"
                "\nПосещено URL: %s / %s"
                "\nОбработано страниц: %s"
                "\nВ очереди: %s"
                "\nОшибок: %s"
                "\nСкорость: %.2f страниц/сек"
                "\nОценка оставшегося времени: %.2f сек."
                "\nАктивные задачи: %s"
                "\n\nRate limiting:"
                "\nСкорость запросов: %s req/sec"
                "\nСредняя задержка: %s сек."
                "\nЗаблокировано robots.txt: %s",
                progress_percent,
                len(self.visited_urls),
                max_pages,
                len(self.processed_urls),
                stats["queued"],
                len(self.failed_urls),
                speed,
                eta,
                active_tasks,
                crawler_stats["current_speed_req_sec"],
                crawler_stats["rate_limiter"]["average_delay"],
                crawler_stats["blocked_by_robots"]
            )
            
        return self.processed_urls
    
    async def save_page(self, data: dict):
        
        if self.storage is None:
            return

        try:
            await self.storage_retry_strategy.execute_with_retry(self.storage.save, data)

            self.saved_count += 1

        except Exception as e:
            
            url = data.get("url", "")
            error = f"Ошибка сохранения {url}: {e}"

            self.storage_errors.append(error)

            logger.error(error)

    def get_stats(self):
    
        rate_stats = self.rate_limiter.get_stats()
        robots_stats = self.robots_parser.get_stats()
        retry_stats = self.retry_strategy.get_stats()

        current_speed = 0.0

        if len(self.request_times) >= 2:
            elapsed = self.request_times[-1] - self.request_times[0]

            if elapsed > 0:
                current_speed = len(self.request_times) / elapsed

        stats = {
            "current_speed_req_sec": round(current_speed, 2),
            "rate_limiter": rate_stats,
            "robots": robots_stats,
            "retry": retry_stats,
            "blocked_by_robots": len(self.blocked_by_robots),
            "storage": {
                "saved_count": self.saved_count,
                "storage_errors": len(self.storage_errors)
            }
        }

        return stats

    async def close(self):
        
        await self.session.close()

        if self.storage is not None:
            await self.storage.close()

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
            "https://httpbin.org/delay/1"
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
    
    logging.basicConfig(
        level = logging.INFO,
        format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    asyncio.run(main())