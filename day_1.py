import asyncio
import aiohttp
import time
class AsyncCrawler:
    
    def __init__(self, max_concurrent: int = 10):
        
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore (max_concurrent)

        timeout = aiohttp.ClientTimeout (
            total = 10,
            connect = 5,
            sock_read = 6
        )
        connector = aiohttp.TCPConnector(
        limit=max_concurrent,
        limit_per_host=max_concurrent
        )
        self.session = aiohttp.ClientSession (
            timeout=timeout,
            connector=connector)


    async def fetch_url (self, url: str) -> str:
        
        async with self.semaphore:
            print (f"Начинается загрузка {url}")

            try:
                async with self.session.get (url) as response:
                    response.raise_for_status()

                    html = await response. text()
                    
                    print (f"Успешно загружно {url}") 
                    return html
                
            except aiohttp.ClientResponseError as e:
                print (f"Ошибка: {url}. Статус: {e.status}")
                return ""
            
            except asyncio.TimeoutError:
                print(f"Таймаут: {url}")
                return ""

            except aiohttp.ClientError as e:
                print(f"Сетевая ошибка: {url} | {e}")
                return ""
        
    async def fetch_and_parse(self, url: str) -> dict:
        
        html = await self.fetch_url (url)

        if not html:
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {}
            }
        parsed_data = await self.parser.parse_html(html, url)
        
        return {
            "url": parsed_data["url"],
            "title": parsed_data["title"],
            "text": parsed_data["text"],
            "links": parsed_data["links"],
            "metadata": parsed_data["metadata"]
            }
    
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
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/1"
        ]

        crawler = AsyncCrawler(max_concurrent=5)

        start_time = time.perf_counter()
        
        try:
            results = await crawler.fetch_urls(urls)

            end_time = time.perf_counter()
            total_time = end_time - start_time

            print("\nРЕЗУЛЬТАТ:")

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

            print (f"\nПОСЛЕДОВАТЕЛЬНАЯ ЗАГРУЗКА")
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