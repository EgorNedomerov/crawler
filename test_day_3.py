import asyncio
import json
import time
from urllib.parse import urlparse
from day_1 import AsyncCrawler
from day_3 import CrawlerQueue, SemaphoreManager


async def test_queue_priority():
  
    print("\nОчередь с приоритетами")

    queue = CrawlerQueue()

    queue.add_url("https://example.com/low", priority=1)
    queue.add_url("https://example.com/high", priority=10)
    queue.add_url("https://example.com/medium", priority=5)

    first = await queue.get_next()
    second = await queue.get_next()
    third = await queue.get_next()

    expected = [
        "https://example.com/high",
        "https://example.com/medium",
        "https://example.com/low"
    ]

    actual = [first, second, third]

    print(f"Ожидалось: {expected}")
    print(f"Получено:   {actual}")

    assert actual == expected, f"Ошибка приоритетов. Ожидалось {expected}, получено {actual}"


async def test_semaphore_manager():
  
    print("\nSemaphoreManager")

    manager = SemaphoreManager(
        max_concurrent=2,
        max_per_domain=1
    )

    url = "https://example.com/page"

    await manager.acquire(url)

    stats_after_acquire = manager.get_semaphore_stats()

    print(f"Статистика после acquire: {stats_after_acquire}")

    assert stats_after_acquire["active_tasks"] == 1, (
        f"Ожидалось active_tasks=1, получено {stats_after_acquire['active_tasks']}"
    )

    assert stats_after_acquire["domains"] == 1, (
        f"Ожидалось domains=1, получено {stats_after_acquire['domains']}"
    )

    manager.release(url)

    stats_after_release = manager.get_semaphore_stats()

    print(f"Статистика после release: {stats_after_release}")

    assert stats_after_release["active_tasks"] == 0, (
        f"Ожидалось active_tasks=0, получено {stats_after_release['active_tasks']}"
    )

def save_crawl_results(crawler: AsyncCrawler, filename: str):

    data = {
        "processed_urls": crawler.processed_urls,
        "failed_urls": crawler.failed_urls,
        "visited_urls": list(crawler.visited_urls),
        "stats": {
            "processed": len(crawler.processed_urls),
            "failed": len(crawler.failed_urls),
            "visited": len(crawler.visited_urls),
            "max_depth": crawler.max_depth,
            "same_domain_only": crawler.same_domain_only,
            "exclude_patterns": crawler.exclude_patterns,
            "include_patterns": crawler.include_patterns,
        }
    }

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"\nДанные сохранены в файл: {filename}")


def test_no_duplicates(crawler: AsyncCrawler):
   
    print("\nОтсутствие дубликатов")

    visited_count = len(crawler.visited_urls)
    unique_count = len(set(crawler.visited_urls))

    print(f"Всего visited_urls: {visited_count}")
    print(f"Уникальных URL:     {unique_count}")

    assert visited_count == unique_count, (
        f"Найдены дубликаты. Всего: {visited_count}, уникальных: {unique_count}"
    )

def test_depth_limit(crawler: AsyncCrawler):
   
    print("\nОграничение глубины")

    if crawler.queue.url_depths:
        max_found_depth = max(crawler.queue.url_depths.values())
    else:
        max_found_depth = 0

    print(f"Разрешенная max_depth: {crawler.max_depth}")
    print(f"Максимальная найденная глубина: {max_found_depth}")
    print(f"Всего URL с глубиной: {len(crawler.queue.url_depths)}")

    for url, depth in crawler.queue.url_depths.items():
        assert depth <= crawler.max_depth, (
            f"URL превысил глубину: {url}, depth={depth}, max_depth={crawler.max_depth}"
        )


def test_same_domain_filter(crawler: AsyncCrawler, start_urls: list[str]):
   
    print("\nФильтрация по домену")

    allowed_domains = set()

    for url in start_urls:
        parsed_url = urlparse(url)

        if parsed_url.netloc:
            allowed_domains.add(parsed_url.netloc)

    visited_domains = set()

    for url in crawler.visited_urls:
        parsed_url = urlparse(url)
        visited_domains.add(parsed_url.netloc)

    print(f"Разрешенные домены: {allowed_domains}")
    print(f"Посещенные домены:  {visited_domains}")

    if crawler.same_domain_only:
        for url in crawler.visited_urls:
            parsed_url = urlparse(url)
            assert parsed_url.netloc in allowed_domains, (
                f"URL вне разрешенного домена: {url}"
            )

def test_exclude_patterns(crawler: AsyncCrawler):

    print(f"Исключающие паттерны: {crawler.exclude_patterns}")
    print(f"Проверено URL: {len(crawler.visited_urls)}")

    for url in crawler.visited_urls:
        for pattern in crawler.exclude_patterns:
            assert pattern not in url, (
                f"URL содержит запрещенный паттерн: {url}, pattern={pattern}"
            )

async def demo_crawl():
   
    print("\nДемо краулинга")

    start_urls = [
        "https://docs.python.org/3/"
    ]

    crawler = AsyncCrawler(
        max_concurrent=10,
        max_depth=2,
    )   
    start_time = time.perf_counter()
    
    try:
        results = await crawler.crawl(
            start_urls = ["https://docs.python.org/3/"],
            max_pages= 50,
            same_domain_only=True,
            exclude_patterns = [
                    ".pdf",
                    ".jpg",
                    ".jpeg",
                    ".png",
                    ".gif",
                    "/login",
                    "/admin"
                ],
            include_patterns = []
        )

        total_time = time.perf_counter() - start_time

        print("\nКраулинг завершен")
        print(f"Обработано страниц: {len(results)}")
        print(f"Посещено URL: {len(crawler.visited_urls)}")
        print(f"Ошибок: {len(crawler.failed_urls)}")
        print(f"Общее время: {round(total_time, 2)} сек.")

        save_crawl_results(
            crawler=crawler,
            filename="crawl_results.json"
        )

        test_no_duplicates(crawler)
        test_depth_limit(crawler)
        test_same_domain_filter(crawler, start_urls)
        test_exclude_patterns(crawler)

    finally:
        await crawler.close()

async def main():
    await test_queue_priority()
    await test_semaphore_manager()
    await demo_crawl()

if __name__ == "__main__":
    asyncio.run(main())