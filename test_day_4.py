import asyncio
import time
from day_1 import AsyncCrawler
from day_4 import RateLimiter, RobotsParser
import logging

async def test_rate_limiter_one_domain():
    
    print("\nТест rate limiting для одного домена")

    limiter = RateLimiter(requests_per_second=2.0, per_domain=True)

    start = time.perf_counter()

    await limiter.acquire("example.com")
    await limiter.acquire("example.com")
    await limiter.acquire("example.com")

    elapsed = time.perf_counter() - start

    print(f"Время выполнения: {round(elapsed, 2)} сек.")
    print(f"Статистика: {limiter.get_stats()}")

    assert elapsed >= 1.0, f"RateLimiter не выдержал задержку, elapsed={elapsed}"

async def test_rate_limiter_different_domains():
    
    print("\nТест rate limiting для разных доменов")

    limiter = RateLimiter(requests_per_second=1.0, per_domain=True)

    start = time.perf_counter()

    await limiter.acquire("example.com")
    await limiter.acquire("python.org")

    elapsed = time.perf_counter() - start

    print(f"Время выполнения: {round(elapsed, 2)} сек.")
    print(f"Статистика: {limiter.get_stats()}")

    assert elapsed < 0.5, f"Разные домены не должны ждать друг друга, elapsed={elapsed}"

async def test_robots_blocked_url(): # Протестировать блокировку запрещённых URL сделал отдельным методом, python.org не блокировал url
    
    print("\nБлокировка запрещённого URL")

    robots = RobotsParser()

    base_url = "https://example.com"

    await robots.fetch_robots(base_url)

    parser = robots.robots_cache[base_url]
    parser.parse([
        "User-agent: *",
        "Disallow: /private"
    ])

    blocked_url = "https://example.com/private/page"

    can_fetch = robots.can_fetch(blocked_url, "*")

    print(f"Можно загрузить {blocked_url}: {can_fetch}")
    print(f"Статистика robots: {robots.get_stats()}")

    assert can_fetch is False, "Запрещённый URL не был заблокирован"

async def test_robots_parser():
    
    print("\nТест robots.txt")

    robots = RobotsParser()

    result = await robots.fetch_robots("https://www.python.org")

    print(f"Результат загрузки robots.txt: {result}")

    allowed_url = "https://www.python.org/"
    can_fetch = robots.can_fetch(allowed_url, "MyBot/1.0")

    print(f"Можно загрузить {allowed_url}: {can_fetch}")
    print(f"Статистика robots: {robots.get_stats()}")

    assert isinstance(can_fetch, bool)

async def demo_crawler_day_4():
    
    print("\nДемо Day 4")

    crawler = AsyncCrawler(
        max_concurrent=5,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.5,
        user_agent="MyCrawler/1.0"
    )

    try:
        results = await crawler.crawl(
            start_urls=["https://docs.python.org/3/"],
            max_pages=10,
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

        print(f"\nОбработано страниц: {len(results)}")
        print(f"Rate stats: {crawler.get_stats()}")

    finally:
        await crawler.close()

async def main():
    await test_rate_limiter_one_domain()
    await test_rate_limiter_different_domains()
    await test_robots_parser()
    await test_robots_blocked_url()
    await demo_crawler_day_4()

logging.basicConfig(
        level = logging.INFO,
        format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

if __name__ == "__main__":
    asyncio.run(main())