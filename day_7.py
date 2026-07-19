import json
import time
import html
import logging
import aiohttp
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urlparse
from logging.handlers import RotatingFileHandler
from day_1 import AsyncCrawler
from day_6 import JSONStorage, CSVStorage, SQLiteStorage
logger = logging.getLogger(__name__)

def setup_logging(
    filename: str = "crawler.log",
    level: str = "INFO",
    max_bytes: int = 1_000_000,
    backup_count: int = 3
):
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        filename,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

class SitemapParser:

    def __init__(
        self,
        session=None,
        user_agent: str = "MyCrawler/1.0",
        max_depth: int = 5
    ):
        self.session = session
        self.user_agent = user_agent
        self.max_depth = max_depth
        self.visited_sitemaps = set()

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:

        return await self._fetch_sitemap_recursive(sitemap_url, 0)

    async def _fetch_sitemap_recursive(self, sitemap_url: str, depth: int) -> list[str]:

        if depth > self.max_depth:
            logger.warning("Превышена глубина sitemap: %s", sitemap_url)
            return []

        if sitemap_url in self.visited_sitemaps:
            return []

        self.visited_sitemaps.add(sitemap_url)

        headers = {
            "User-Agent": self.user_agent
        }
        try:
            if self.session is not None:
                async with self.session.get(sitemap_url, headers=headers) as response:
                    if response.status >= 400:
                        logger.warning(
                            "Sitemap не загружен: %s | статус %s",
                            sitemap_url,
                            response.status
                        )
                        return []

                    xml_text = await response.text()
            else:
                async with aiohttp.ClientSession() as session:
                    async with session.get(sitemap_url, headers=headers) as response:
                        if response.status >= 400:
                            logger.warning(
                                "Sitemap не загружен: %s | статус %s",
                                sitemap_url,
                                response.status
                            )
                            return []

                        xml_text = await response.text()

            root = ET.fromstring(xml_text)

            root_tag = root.tag

            if "}" in root_tag:
                root_tag = root_tag.split("}", 1)[1]

            urls = []

            if root_tag == "urlset":
                for element in root.iter():
                    tag = element.tag

                    if "}" in tag:
                        tag = tag.split("}", 1)[1]

                    if tag == "loc" and element.text:
                        urls.append(element.text.strip())

            elif root_tag == "sitemapindex":
                for element in root.iter():
                    tag = element.tag

                    if "}" in tag:
                        tag = tag.split("}", 1)[1]

                    if tag == "loc" and element.text:
                        nested_sitemap_url = element.text.strip()
                        nested_urls = await self._fetch_sitemap_recursive(
                            nested_sitemap_url,
                            depth + 1
                        )
                        urls.extend(nested_urls)

            else:
                logger.warning("Неизвестный тип sitemap: %s", root_tag)

            return list(dict.fromkeys(urls))

        except Exception as e:
            logger.warning(
                "Ошибка обработки sitemap %s: %s",
                sitemap_url,
                e
            )
            return []

class CrawlerStats:

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.started_at = None
        self.finished_at = None
        self.total_pages = 0
        self.successful = 0
        self.failed = 0
        self.status_codes = {}
        self.domains = {}

    def start(self):

        self.start_time = time.perf_counter()
        self.started_at = datetime.now().isoformat()

    def finish(self):

        self.end_time = time.perf_counter()
        self.finished_at = datetime.now().isoformat()

    def record_success(self, url: str, status_code: int = 200):

        self.total_pages += 1
        self.successful += 1

        status_key = str(status_code)

        if status_key not in self.status_codes:
            self.status_codes[status_key] = 0

        self.status_codes[status_key] += 1

        domain = urlparse(url).netloc or "unknown"

        if domain not in self.domains:
            self.domains[domain] = 0

        self.domains[domain] += 1

    def record_failure(self, url: str, status_code: int = 0):

        self.total_pages += 1
        self.failed += 1

        status_key = str(status_code)

        if status_key not in self.status_codes:
            self.status_codes[status_key] = 0

        self.status_codes[status_key] += 1

        domain = urlparse(url).netloc or "unknown"

        if domain not in self.domains:
            self.domains[domain] = 0

        self.domains[domain] += 1

    def to_dict(self) -> dict:

        if self.start_time is None:
            duration = 0.0
        else:
            end_time = self.end_time or time.perf_counter()
            duration = end_time - self.start_time

        if duration > 0:
            pages_per_second = self.total_pages / duration
        else:
            pages_per_second = 0.0

        top_domains = dict(
            sorted(
                self.domains.items(),
                key=lambda item: item[1],
                reverse=True
            )[:10]
        )

        return {
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration": round(duration, 2),
            "total_pages": self.total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "pages_per_second": round(pages_per_second, 2),
            "status_codes": self.status_codes,
            "top_domains": top_domains
        }

class AdvancedCrawler:

    def __init__(
        self,
        start_urls: list[str],
        sitemaps: list[str] | None = None,
        max_pages: int = 100,
        max_depth: int = 2,
        max_concurrent: int = 10,
        max_per_domain: int = 2,
        rate_limit: float = 1.0,
        respect_robots: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str = "MyCrawler/1.0",
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
        same_domain_only: bool = True,
        storage=None
    ):
        self.start_urls = start_urls
        self.sitemaps = sitemaps or []
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.max_concurrent = max_concurrent
        self.max_per_domain = max_per_domain
        self.rate_limit = rate_limit
        self.respect_robots = respect_robots
        self.min_delay = min_delay
        self.jitter = jitter
        self.user_agent = user_agent
        self.exclude_patterns = exclude_patterns or []
        self.include_patterns = include_patterns or []
        self.same_domain_only = same_domain_only
        self.storage = storage

        self.crawler = AsyncCrawler(
            max_concurrent=max_concurrent,
            max_depth=max_depth,
            max_per_domain=max_per_domain,
            requests_per_second=rate_limit,
            respect_robots=respect_robots,
            min_delay=min_delay,
            jitter=jitter,
            user_agent=user_agent,
            storage=storage
        )

        self.sitemap_parser = SitemapParser(
            session=self.crawler.session,
            user_agent=user_agent
        )

        self.stats = CrawlerStats()
        self.results = {}

    @classmethod
    def from_config(cls, filename: str):

        with open(filename, "r", encoding="utf-8") as file:
            config = json.load(file)

        logging_config = config.get("logging", {})

        setup_logging(
            filename=logging_config.get("filename", "crawler.log"),
            level=logging_config.get("level", "INFO"),
            max_bytes=logging_config.get("max_bytes", 1_000_000),
            backup_count=logging_config.get("backup_count", 3)
        )

        storage_config = config.get("storage", {})
        storage_type = storage_config.get("type", "json")
        storage_filename = storage_config.get("filename", "results.jsonl")

        if storage_type == "json":
            storage = JSONStorage(
                filename=storage_filename,
                indent=storage_config.get("indent"),
                mode=storage_config.get("mode", "jsonl")
            )

        elif storage_type == "csv":
            storage = CSVStorage(
                filename=storage_filename
            )

        elif storage_type == "sqlite":
            storage = SQLiteStorage(
                filename=storage_filename,
                batch_size=storage_config.get("batch_size", 10)
            )

        elif storage_type in ["none", None]:
            storage = None

        else:
            raise ValueError(f"Неизвестный тип storage: {storage_type}")

        return cls(
            start_urls=config.get("start_urls", []),
            sitemaps=config.get("sitemaps", []),
            max_pages=config.get("max_pages", 100),
            max_depth=config.get("max_depth", 2),
            max_concurrent=config.get("max_concurrent", 10),
            max_per_domain=config.get("max_per_domain", 2),
            rate_limit=config.get("rate_limit", 1.0),
            respect_robots=config.get("respect_robots", True),
            min_delay=config.get("min_delay", 0.0),
            jitter=config.get("jitter", 0.0),
            user_agent=config.get("user_agent", "MyCrawler/1.0"),
            exclude_patterns=config.get("exclude_patterns", []),
            include_patterns=config.get("include_patterns", []),
            same_domain_only=config.get("same_domain_only", True),
            storage=storage
        )

    async def crawl(self):

        logger.info("AdvancedCrawler запущен")
        
        self.stats = CrawlerStats()
        self.results = {}
        self.sitemap_parser.visited_sitemaps = set()

        self.stats.start()

        urls = list(self.start_urls)

        for sitemap_url in self.sitemaps:
            logger.info("Загрузка sitemap: %s", sitemap_url)
            sitemap_urls = await self.sitemap_parser.fetch_sitemap(sitemap_url)
            logger.info("Из sitemap получено URL: %s", len(sitemap_urls))
            urls.extend(sitemap_urls)

        urls = list(dict.fromkeys(urls))

        if not urls:
            logger.warning("Нет URL для краулинга")
            self.stats.finish()
            return {}

        self.results = await self.crawler.crawl(
            start_urls=urls,
            max_pages=self.max_pages,
            same_domain_only=self.same_domain_only,
            exclude_patterns=self.exclude_patterns,
            include_patterns=self.include_patterns
        )

        for url, data in self.crawler.processed_urls.items():
            status_code = data.get(
                "status_code",
                self.crawler.last_status_codes.get(url, 200)
            )

            self.stats.record_success(url, status_code)

        for url in self.crawler.failed_urls:
            status_code = self.crawler.last_status_codes.get(url, 0)
            self.stats.record_failure(url, status_code)

        self.stats.finish()

        logger.info(
            "AdvancedCrawler завершён. Обработано: %s. Успешно: %s. Ошибок: %s",
            self.stats.total_pages,
            self.stats.successful,
            self.stats.failed
        )

        return self.results

    def get_stats(self) -> dict:

        stats = self.stats.to_dict()

        stats["crawler"] = self.crawler.get_stats()
        stats["sitemap"] = {
            "visited_sitemaps": len(self.sitemap_parser.visited_sitemaps)
        }

        return stats

    def export_to_json(self, filename: str):

        with open(filename, "w", encoding="utf-8") as file:
            json.dump(
                self.get_stats(),
                file,
                ensure_ascii=False,
                indent=4
            )

        logger.info("Статистика экспортирована в JSON: %s", filename)

    def export_to_html_report(self, filename: str):

        stats = self.get_stats()

        total = stats["total_pages"]
        successful = stats["successful"]
        failed = stats["failed"]

        if total > 0:
            success_percent = round((successful / total) * 100, 2)
            failed_percent = round((failed / total) * 100, 2)
        else:
            success_percent = 0
            failed_percent = 0

        status_rows = ""

        for status, count in stats["status_codes"].items():
            status_rows += (
                f"<tr>"
                f"<td>{html.escape(str(status))}</td>"
                f"<td>{count}</td>"
                f"</tr>\n"
            )

        domain_rows = ""

        for domain, count in stats["top_domains"].items():
            domain_rows += (
                f"<tr>"
                f"<td>{html.escape(str(domain))}</td>"
                f"<td>{count}</td>"
                f"</tr>\n"
            )

        html_report = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Crawler Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 30px;
            background: #f5f5f5;
            color: #222;
        }}

        h1, h2 {{
            color: #333;
        }}

        .card {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}

        th, td {{
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
        }}

        th {{
            background: #eeeeee;
        }}

        .bar-wrapper {{
            width: 100%;
            background: #eeeeee;
            border-radius: 5px;
            overflow: hidden;
            margin: 8px 0 16px 0;
        }}

        .bar {{
            padding: 8px;
            color: white;
            background: #4a90e2;
            min-width: 35px;
        }}

        .bar-error {{
            background: #d9534f;
        }}

        .metric {{
            display: grid;
            grid-template-columns: 260px 1fr;
            gap: 8px;
            margin: 6px 0;
        }}
    </style>
</head>
<body>
    <h1>Отчёт краулера</h1>

    <div class="card">
        <h2>Общая статистика</h2>
        <div class="metric"><strong>Начало:</strong><span>{html.escape(str(stats["started_at"]))}</span></div>
        <div class="metric"><strong>Завершение:</strong><span>{html.escape(str(stats["finished_at"]))}</span></div>
        <div class="metric"><strong>Время работы:</strong><span>{stats["duration"]} сек.</span></div>
        <div class="metric"><strong>Всего обработано:</strong><span>{stats["total_pages"]}</span></div>
        <div class="metric"><strong>Успешно:</strong><span>{stats["successful"]}</span></div>
        <div class="metric"><strong>Ошибок:</strong><span>{stats["failed"]}</span></div>
        <div class="metric"><strong>Средняя скорость:</strong><span>{stats["pages_per_second"]} страниц/сек.</span></div>
    </div>

    <div class="card">
        <h2>Визуализация статистики</h2>

        <p>Успешные запросы: {success_percent}%</p>
        <div class="bar-wrapper">
            <div class="bar" style="width: {success_percent}%">{successful}</div>
        </div>

        <p>Ошибки: {failed_percent}%</p>
        <div class="bar-wrapper">
            <div class="bar bar-error" style="width: {failed_percent}%">{failed}</div>
        </div>
    </div>

    <div class="card">
        <h2>Распределение по статус-кодам</h2>
        <table>
            <tr>
                <th>Статус-код</th>
                <th>Количество</th>
            </tr>
            {status_rows}
        </table>
    </div>

    <div class="card">
        <h2>Топ доменов</h2>
        <table>
            <tr>
                <th>Домен</th>
                <th>Количество страниц</th>
            </tr>
            {domain_rows}
        </table>
    </div>
</body>
</html>"""

        with open(filename, "w", encoding="utf-8") as file:
            file.write(html_report)

        logger.info("HTML-отчёт создан: %s", filename)

    async def close(self):
        await self.crawler.close()