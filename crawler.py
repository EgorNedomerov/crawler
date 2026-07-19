import argparse
import asyncio
import logging
from day_7 import AdvancedCrawler, setup_logging
from day_6 import JSONStorage, CSVStorage, SQLiteStorage
logger = logging.getLogger(__name__)

async def main():
    
    parser = argparse.ArgumentParser(
        description="Advanced async web crawler"
    )

    parser.add_argument(
        "--urls",
        nargs="+",
        help="Стартовые URL"
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Максимальное количество страниц"
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=2,
        help="Максимальная глубина"
    )

    parser.add_argument(
        "--output",
        default="results.jsonl",
        help="Файл для сохранения результатов"
    )

    parser.add_argument(
        "--config",
        help="Конфигурационный JSON-файл"
    )

    parser.add_argument(
        "--respect-robots",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Соблюдать robots.txt"
    )

    parser.add_argument(
        "--rate-limit",
        type=float,
        default=1.0,
        help="Лимит запросов в секунду"
    )

    parser.add_argument(
        "--stats-json",
        default="stats.json",
        help="Файл JSON-статистики"
    )

    parser.add_argument(
        "--html-report",
        default="report.html",
        help="Файл HTML-отчёта"
    )

    args = parser.parse_args()

    if args.config:
        crawler = AdvancedCrawler.from_config(args.config)

    else:
        setup_logging(
            filename="crawler.log",
            level="INFO"
        )

        if not args.urls:
            raise ValueError("Нужно указать --urls или --config")

        if args.output.endswith(".csv"):
            storage = CSVStorage(args.output)

        elif (
            args.output.endswith(".db")
            or args.output.endswith(".sqlite")
            or args.output.endswith(".sqlite3")
        ):
            storage = SQLiteStorage(args.output)

        elif args.output.endswith(".json"):
            storage = JSONStorage(
                filename=args.output,
                indent=4,
                mode="json_array"
            )

        else:
            storage = JSONStorage(
                filename=args.output,
                mode="jsonl"
            )

        crawler = AdvancedCrawler(
            start_urls=args.urls,
            max_pages=args.max_pages,
            max_depth=args.max_depth,
            rate_limit=args.rate_limit,
            respect_robots=args.respect_robots,
            storage=storage
        )

    try:
        await crawler.crawl()

        stats = crawler.get_stats()

        print(f"Обработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")
        print(f"Скорость: {stats['pages_per_second']} страниц/сек.")

        crawler.export_to_json(args.stats_json)
        crawler.export_to_html_report(args.html_report)

    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())