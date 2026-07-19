import asyncio
import json
import os
from aiohttp import web
from day_7 import SitemapParser, CrawlerStats, AdvancedCrawler, setup_logging

def remove_file(filename: str):
    
    if os.path.exists(filename):
        os.remove(filename)

async def test_sitemap_parser():

    print("\nТест SitemapParser")

    async def sitemap_index(request):

        text = """<?xml version="1.0" encoding="UTF-8"?>
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <sitemap>
                <loc>http://127.0.0.1:8081/sitemap-pages.xml</loc>
            </sitemap>
        </sitemapindex>
        """
        return web.Response(text=text, content_type="application/xml")

    async def sitemap_pages(request):

        text = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://example.com/page1</loc>
            </url>
            <url>
                <loc>https://example.com/page2</loc>
            </url>
        </urlset>
        """
        return web.Response(text=text, content_type="application/xml")

    app = web.Application()
    app.router.add_get("/sitemap.xml", sitemap_index)
    app.router.add_get("/sitemap-pages.xml", sitemap_pages)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 8081)
    await site.start()

    try:
        parser = SitemapParser()
        urls = await parser.fetch_sitemap("http://127.0.0.1:8081/sitemap.xml")

        print(urls)

        assert len(urls) == 2
        assert "https://example.com/page1" in urls
        assert "https://example.com/page2" in urls

    finally:
        await runner.cleanup()

def test_crawler_stats():

    print("\nТест CrawlerStats")

    stats = CrawlerStats()

    stats.start()
    stats.record_success("https://example.com/page1", 200)
    stats.record_success("https://example.com/page2", 200)
    stats.record_failure("https://example.com/error", 500)
    stats.finish()

    result = stats.to_dict()

    print(result)

    assert result["total_pages"] == 3
    assert result["successful"] == 2
    assert result["failed"] == 1
    assert result["status_codes"]["200"] == 2
    assert result["status_codes"]["500"] == 1
    assert result["top_domains"]["example.com"] == 3

async def test_export_reports():

    print("\nТест экспорта JSON и HTML")

    json_filename = "test_stats.json"
    html_filename = "test_report.html"

    remove_file(json_filename)
    remove_file(html_filename)

    crawler = AdvancedCrawler(
        start_urls=["https://example.com"],
        max_pages=1,
        max_depth=0,
        respect_robots=False,
        storage=None
    )
    crawler.stats.start()
    crawler.stats.record_success("https://example.com", 200)
    crawler.stats.finish()

    crawler.export_to_json(json_filename)
    crawler.export_to_html_report(html_filename)

    assert os.path.exists(json_filename)
    assert os.path.exists(html_filename)

    with open(json_filename, "r", encoding="utf-8") as file:
        data = json.load(file)

    assert data["total_pages"] == 1
    assert data["successful"] == 1

    await crawler.close()

    remove_file(json_filename)
    remove_file(html_filename)

async def test_advanced_crawler_basic():

    print("\nТест AdvancedCrawler basic")

    crawler = AdvancedCrawler(
        start_urls=["https://example.com"],
        max_pages=1,
        max_depth=0,
        respect_robots=False,
        storage=None
    )
    try:
        await crawler.crawl()

        stats = crawler.get_stats()

        print(stats)

        assert stats["total_pages"] >= 1
        assert "successful" in stats
        assert "failed" in stats
        assert "status_codes" in stats
        assert "top_domains" in stats

    finally:
        await crawler.close()

async def main():

    setup_logging(
        filename="test_day_7.log",
        level="INFO"
    )
    await test_sitemap_parser()
    test_crawler_stats()
    await test_export_reports()
    await test_advanced_crawler_basic()

if __name__ == "__main__":
    asyncio.run(main())