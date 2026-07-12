from day_1 import AsyncCrawler
from day_5 import RetryStrategy, TransientError, NetworkError, PermanentError
import json
import time
import asyncio

async def test_retry_transient_error():
    
    print(f"\nТест повтора TransientError")

    strategy = RetryStrategy(
        max_retries=2,
        backoff_factor=2.0,
        retry_on=[TransientError, NetworkError]
    ) 

    attempts = {"count": 0}
    
    async def unstable_function():
        attempts["count"] += 1

        if attempts["count"] < 2:
            raise TransientError("Временная ошибка", url="https://example.com")

        return "success"
    
    result = await strategy.execute_with_retry(unstable_function)

    print(f"Результат: {result}")
    print(f"Количество попыток: {attempts['count']}")
    print(f"Статистика: {strategy.get_stats()}")

    assert result == "success"
    assert attempts["count"] == 2
    assert strategy.successful_retries == 1

async def test_no_retry_permanent_error():
    
    print("\nТест отсутствия повтора PermanentError")

    strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=2.0,
        retry_on=[TransientError, NetworkError]
    )

    attempts = {"count": 0}

    async def permanent_fail():
        attempts["count"] += 1
        raise PermanentError(
            "404 Not Found",
            url="https://example.com/missing",
            status=404
        )

    try:
        await strategy.execute_with_retry(permanent_fail)
    except PermanentError:
        print("PermanentError не повторяется")

    print(f"Количество попыток: {attempts['count']}")
    print(f"Статистика: {strategy.get_stats()}")

    assert attempts["count"] == 1
    assert "https://example.com/missing" in strategy.permanent_error_urls

async def test_exponential_backoff():
    
    print("\nТест экспоненциального backoff")

    strategy = RetryStrategy(
        max_retries=2,
        backoff_factor=2.0,
        retry_on=[TransientError, NetworkError]
    )

    attempts = {"count": 0}

    async def always_fail():
        
        attempts["count"] += 1
        raise TransientError(
            "Временная ошибка",
            url="https://example.com/unstable"
        )
    
    start = time.perf_counter()

    try:
        await strategy.execute_with_retry(always_fail)
    except TransientError:
        print("TransientError после всех повторов")

    elapsed = time.perf_counter() - start

    print(f"Количество попыток: {attempts['count']}")
    print(f"Время выполнения: {round(elapsed, 2)} сек.")
    print(f"Retry times: {strategy.retry_times}")

    assert attempts["count"] == 3
    assert len(strategy.retry_times) == 2
    assert strategy.retry_times[1] > strategy.retry_times[0]

async def test_timeout_retry():
    
    print("\nТест повтора при таймауте")
    
    strategy = RetryStrategy(
        max_retries=2,
        backoff_factor=2.0,
        retry_on=[TransientError, NetworkError]
    )

    attempts = {"count": 0}
    
    async def timeout_function():
        attempts["count"] += 1
        raise TransientError(
            "Таймаут запроса",
            url="https://example.com/timeout"
        )
    
    try:
        await strategy.execute_with_retry(timeout_function)
    except TransientError:
        print("Таймаут завершился после всех попыток")

    stats = strategy.get_stats()

    print(f"Количество попыток: {attempts['count']}")
    print(f"Статистика: {stats}")

    assert attempts["count"] == 3
    assert stats["error_counts"].get("TransientError", 0) == 3
    assert len(stats["attempt_logs"]) == 3

async def test_http_404_no_retry():
    
    print("\nТест HTTP 404 без повтора")

    crawler = AsyncCrawler(
        max_concurrent=2,
        max_retries=3,
        backoff_factor=2.0,
        respect_robots=False
    )

    try:
        html = await crawler.fetch_url("https://www.python.org/this-page-does-not-exist-404-test")

        stats = crawler.get_stats()
        retry_stats = stats["retry"]

        print(f"HTML: {html}")
        print(f"Retry stats: {retry_stats}")

        assert html == ""
        assert "https://www.python.org/this-page-does-not-exist-404-test" in crawler.failed_urls
        assert retry_stats["error_counts"].get("PermanentError", 0) == 1

    finally:
        await crawler.close()

async def test_http_503_retry():
    
    print("\nТест HTTP 503 с повторами")

    crawler = AsyncCrawler(
        max_concurrent=2,
        max_retries=2,
        backoff_factor=2.0,
        respect_robots=False
    )

    try:
        html = await crawler.fetch_url("https://httpbin.org/status/503")

        stats = crawler.get_stats()
        retry_stats = stats["retry"]

        print(f"HTML: {html}")
        print(f"Retry stats: {retry_stats}")

        error_counts = retry_stats["error_counts"]

        errors_count = (error_counts.get("TransientError", 0) + error_counts.get("NetworkError", 0))

        assert html == ""
        assert "https://httpbin.org/status/503" in crawler.failed_urls
        assert errors_count == 3

    finally:
        await crawler.close()

def save_error_report(crawler: AsyncCrawler, filename: str):
    
    stats = crawler.get_stats()

    report = {
        "failed_urls": crawler.failed_urls,
        "last_errors": crawler.last_errors,
        "retry_stats": stats["retry"],
        "permanent_error_urls": stats["retry"]["permanent_error_urls"]
    }

    with open(filename, "w", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=4)

    print(f"\nОтчёт об ошибках сохранён: {filename}")

async def demo_day_5():
    print("\nДемо Day 5")

    crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=1,
        max_retries=2,
        backoff_factor=2.0,
        respect_robots=False
    )

    urls = [
        "https://httpbin.org/status/404",
        "https://httpbin.org/status/503",
        "https://httpbin.org/status/500",
        "https://httpbin.org/delay/15"
    ]

    try:
        for url in urls:
            await crawler.fetch_url(url)

        print("\nСтатистика ошибок:")
        print(crawler.get_stats()["retry"])

        save_error_report(
            crawler=crawler,
            filename="error_report.json"
        )

    finally:
        await crawler.close()

async def main():
    await test_retry_transient_error()
    await test_no_retry_permanent_error()
    await test_exponential_backoff()
    await test_timeout_retry()
    await test_http_404_no_retry()
    await test_http_503_retry()
    await demo_day_5()

if __name__ == "__main__":
    asyncio.run(main())