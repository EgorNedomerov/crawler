import asyncio
import json
import csv
import os
import aiosqlite
from day_1 import AsyncCrawler
from day_6 import DataStorage, JSONStorage, CSVStorage, SQLiteStorage

TEST_DATA = {
    "url": "https://example.com",
    "title": "Example Domain",
    "text": "Example text",
    "links": ["https://example.com/about", "https://example.com/contact"],
    "metadata": {
        "description": "Example description"
    },
    "crawled_at": "2026-01-01T12:00:00",
    "status_code": 200,
    "content_type": "text/html"
}

def remove_file(filename: str):
    if os.path.exists(filename):
        os.remove(filename)

async def test_json_storage():
    
    print("\nТест JSONStorage")

    filename = "test_results.jsonl"
    remove_file(filename)

    storage = JSONStorage(filename)

    await storage.save(TEST_DATA)
    await storage.close()

    with open(filename, "r", encoding="utf-8") as file:
        lines = file.readlines()

    assert len(lines) == 1

    saved_data = json.loads(lines[0])

    print(saved_data)

    assert saved_data["url"] == TEST_DATA["url"]
    assert saved_data["title"] == TEST_DATA["title"]
    assert saved_data["status_code"] == 200

    remove_file(filename)

async def test_csv_storage():
    
    print("\nТест CSVStorage")

    filename = "test_results.csv"
    remove_file(filename)

    storage = CSVStorage(filename)

    await storage.save(TEST_DATA)
    await storage.close()

    with open(filename, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    assert len(rows) == 1

    saved_data = rows[0]

    print(saved_data)

    assert saved_data["url"] == TEST_DATA["url"]
    assert saved_data["title"] == TEST_DATA["title"]
    assert saved_data["status_code"] == "200"

    links = json.loads(saved_data["links"])
    metadata = json.loads(saved_data["metadata"])

    assert links == TEST_DATA["links"]
    assert metadata == TEST_DATA["metadata"]

    remove_file(filename)

async def test_sqlite_storage():
    
    print("\nТест SQLiteStorage")

    filename = "test_crawler.db"
    remove_file(filename)

    storage = SQLiteStorage(filename, batch_size=2)

    await storage.init_db()
    await storage.save(TEST_DATA)
    await storage.close()

    connection = await aiosqlite.connect(filename)

    cursor = await connection.execute("""
        SELECT url, title, status_code, content_type
        FROM pages
        WHERE url = ?
    """, (TEST_DATA["url"],))

    row = await cursor.fetchone()

    await cursor.close()
    await connection.close()

    print(row)

    assert row is not None
    assert row[0] == TEST_DATA["url"]
    assert row[1] == TEST_DATA["title"]
    assert row[2] == 200
    assert row[3] == TEST_DATA["content_type"]

    remove_file(filename)

class BrokenStorage(DataStorage):

    async def save(self, data: dict):
        raise OSError("Искусственная ошибка записи")

    async def close(self):
        pass

async def test_storage_error_handling():
    
    print("\nТест обработки ошибки сохранения")

    storage = BrokenStorage()

    crawler = AsyncCrawler(
        storage=storage,
        respect_robots=False,
        max_retries=1,
        backoff_factor=1.0
    )
    try:
        await crawler.save_page(TEST_DATA)

        stats = crawler.get_stats()

        print(stats["storage"])

        assert len(crawler.storage_errors) == 1
        assert stats["storage"]["storage_errors"] == 1

    finally:
        await crawler.close()

async def demo_json_crawl():
    
    print("\nДемо JSONStorage + AsyncCrawler")

    filename = "demo_results.jsonl"
    remove_file(filename)

    storage = JSONStorage(filename)

    crawler = AsyncCrawler(
        storage=storage,
        max_concurrent=2,
        max_depth=1,
        max_retries=1,
        backoff_factor=1.0,
        respect_robots=False
    )

    try:
        await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=1,
            same_domain_only=True
        )

        print(crawler.get_stats())

    finally:
        await crawler.close()

async def demo_csv_crawl():
    
    print("\nДемо CSVStorage + AsyncCrawler")

    filename = "demo_results.csv"
    remove_file(filename)

    storage = CSVStorage(filename)

    crawler = AsyncCrawler(
        storage=storage,
        max_concurrent=2,
        max_depth=1,
        max_retries=1,
        backoff_factor=1.0,
        respect_robots=False
    )

    try:
        await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=1,
            same_domain_only=True
        )

        print(crawler.get_stats())

    finally:
        await crawler.close()

async def demo_sqlite_crawl():

    print("\nДемо SQLiteStorage + AsyncCrawler")

    filename = "demo_crawler.db"
    remove_file(filename)

    storage = SQLiteStorage(filename, batch_size=2)
    await storage.init_db()

    crawler = AsyncCrawler(
        storage=storage,
        max_concurrent=2,
        max_depth=1,
        max_retries=1,
        backoff_factor=1.0,
        respect_robots=False
    )

    try:
        await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=1,
            same_domain_only=True
        )

        await storage.close()

        storage = SQLiteStorage(filename)
        await storage.init_db()

        saved_pages = await storage.read_all()

        print(crawler.get_stats())
        print(f"Прочитано из SQLite: {len(saved_pages)}")
        print(saved_pages)

        await storage.close()

        crawler.storage = None

    finally:
        await crawler.close()

async def main():
    await test_json_storage()
    await test_csv_storage()
    await test_sqlite_storage()
    await test_storage_error_handling()
    await demo_json_crawl()
    await demo_csv_crawl()
    await demo_sqlite_crawl()

if __name__ == "__main__":
    asyncio.run(main())