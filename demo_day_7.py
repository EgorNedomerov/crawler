import asyncio

async def main():

    from crawler import AdvancedCrawler
    
    crawler = AdvancedCrawler.from_config("config.json")

    try:
        await crawler.crawl()

        stats = crawler.get_stats()

        print(f"Обработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")

        crawler.export_to_json("stats.json")
        crawler.export_to_html_report("report.html")

    finally:
        await crawler.close()

asyncio.run(main())