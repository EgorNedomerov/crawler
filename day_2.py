from day_1 import AsyncCrawler
from bs4 import BeautifulSoup
import asyncio
from urllib.parse import urljoin, urlparse

class HTMLParser:
    
    async def parse_html (self, html:str, url:str ) -> dict:
        
        result = {
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

        try:
            soup = BeautifulSoup(html, "lxml")

        except Exception as e:
            print(f"Предупреждение: не удалось распарсить HTML для {url}. Ошибка: {e}")
            return result

        try:
            metadata = self.extract_metadata(soup)
            result["metadata"] = metadata
            result["title"] = metadata.get("title", "")

        except Exception as e:
            print(f"Предупреждение: не удалось извлечь метаданные для {url}. Ошибка: {e}")

        try:
            result["text"] = self.extract_text(soup)

        except Exception as e:
            print(f"Предупреждение: не удалось извлечь текст для {url}. Ошибка: {e}")

        try:
            result["links"] = self.extract_links(soup, url)

        except Exception as e:
            print(f"Предупреждение: не удалось извлечь ссылки для {url}. Ошибка: {e}")

        try:
            result["images"] = self.extract_images(soup, url)

        except Exception as e:
            print(f"Предупреждение: не удалось извлечь изображения для {url}. Ошибка: {e}")

        try:
            result["headings"] = self.extract_headings(soup)

        except Exception as e:
            print(f"Предупреждение: не удалось извлечь заголовки для {url}. Ошибка: {e}")

        try:
            result["tables"] = self.extract_tables(soup)

        except Exception as e:
            print(f"Предупреждение: не удалось извлечь таблицы для {url}. Ошибка: {e}")

        try:
            result["lists"] = self.extract_lists(soup)

        except Exception as e:
            print(f"Предупреждение: не удалось извлечь списки для {url}. Ошибка: {e}")

        return result
        
    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        
        links = []

        for tag in soup.find_all("a"):
            href = tag.get("href")

            if not href:
                continue

            absolute_url = urljoin(base_url, href)

            parsed_url = urlparse(absolute_url)

            if parsed_url.scheme in ["http", "https"] and parsed_url.netloc:
                links.append(absolute_url)

        return links
    
    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        
        if selector:
            elements = soup.select(selector)
            text_parts = []

            for element in elements:
                text_parts.append(element.get_text(separator=" ", strip=True))

            return " ".join(text_parts)

        return soup.get_text(separator=" ", strip=True)
    
    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        
        metadata = {
            "title": "",
            "description": "",
            "keywords": ""
        }

        title_tag = soup.find("title")
        if title_tag:
            metadata["title"] = title_tag.get_text(strip=True)

        description_tag = soup.find("meta", attrs={"name": "description"})
        if description_tag:
            metadata["description"] = description_tag.get("content", "")

        keywords_tag = soup.find("meta", attrs={"name": "keywords"})
        if keywords_tag:
            metadata["keywords"] = keywords_tag.get("content", "")

        return metadata

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        
        images = []

        for img in soup.find_all("img"):
            src = img.get("src")

            if not src:
                continue

            absolute_src = urljoin(base_url, src)
            parsed_url = urlparse(absolute_src)

            if parsed_url.scheme in ["http", "https"] and parsed_url.netloc:
                image_data = {
                    "src": absolute_src,
                    "alt": img.get("alt", "")
                }

                images.append(image_data)

        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict]:
        
        headings = []

        for tag in soup.find_all(["h1", "h2", "h3"]):
            heading_data = {
                "tag": tag.name,
                "text": tag.get_text(strip=True)
            }

            headings.append(heading_data)

        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        
        tables = []

        for table in soup.find_all("table"):
            table_data = []

            rows = table.find_all("tr")

            for row in rows:
                row_data = []

                cells = row.find_all(["th", "td"])

                for cell in cells:
                    row_data.append(cell.get_text(strip=True))

                if row_data:
                    table_data.append(row_data)

            if table_data:
                tables.append(table_data)

        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        
        lists = []

        for list_tag in soup.find_all(["ul", "ol"]):
            items = []

            for li in list_tag.find_all("li"):
                items.append(li.get_text(separator=" ", strip=True))

            list_data = {
                "type": list_tag.name,
                "items": items
            }

            lists.append(list_data)

        return lists

if __name__ == "__main__":
    
    async def parsing ():
        
        crawler = AsyncCrawler(max_concurrent=5)
        crawler.parser = HTMLParser()

        urls = [
            "https://example.com",
            "https://www.python.org",
            "https://httpbin.org/html"
        ]

        try:
            print("\n загрука и парсинг страниц")

            tasks = []

            for url in urls:
                task = asyncio.create_task(crawler.fetch_and_parse(url))
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            print("\nРезультат парсинга:")

            for data in results:
                summary = {
                    "url": data["url"],
                    "title": data["title"],
                    "text_length": len(data["text"]),
                    "links_count": len(data["links"]),
                    "links": data["links"][:5],
                    "images_count": len(data.get("images", []))
                }

                print("\n-----------------------------")
                print(summary)

        finally:
            await crawler.close()

    async def test_valid_html():
        
        print("\nПарсинг валидного HTML")

        parser = HTMLParser()

        html = """
        <html>
            <head>
                <title>Test Page</title>
                <meta name="description" content="Test description">
                <meta name="keywords" content="python, html, parser">
            </head>
            <body>
                <h1>Main heading</h1>
                <p>Hello world</p>
                <a href="/about">About</a>
                <img src="/logo.png" alt="Logo">
            </body>
        </html>
        """

        result = await parser.parse_html(html, "https://example.com")

        print("Title:", result["title"])
        print("Text:", result["text"])
        print("Metadata:", result["metadata"])
        print("Links:", result["links"])
        print("Images:", result["images"])

        if result["title"] == "Test Page":
            print("Валидный HTML успешно распарсен")
        else:
            print("Ошибка парсинга валидного HTML")

    async def test_broken_html():
        
        print("\nОбработка битого HTML")

        parser = HTMLParser()

        broken_html = """
        <html>
            <head>
                <title>Broken Page</title>
            <body>
                <h1>Broken heading
                <p>Some text
                <a href="/broken">Broken link
        """

        result = await parser.parse_html(broken_html, "https://example.com")

        print("Title:", result["title"])
        print("Text:", result["text"])
        print("Links:", result["links"])

        print("Битый HTML обработан")

    async def test_links_extraction():
        
        print("\nИзвлечение ссылок")

        parser = HTMLParser()

        html = """
        <html>
            <body>
                <a href="/about">About</a>
                <a href="/contacts">Contacts</a>
                <a href="https://python.org">Python</a>
                <a href="">Empty</a>
                <a>No href</a>
            </body>
        </html>
        """

        soup = BeautifulSoup(html, "lxml")

        links = parser.extract_links(soup, "https://example.com")

        print("Найденные ссылки:")
        for link in links:
            print(link)

        print(f"Количество ссылок: {len(links)}")

    async def test_relative_urls():
        
        print("\nКонвертация относительных ссылок в абсолютные")

        parser = HTMLParser()

        html = """
        <html>
            <body>
                <a href="/about">About</a>
                <a href="contacts">Contacts</a>
                <a href="../news">News</a>
                <a href="https://python.org">Python</a>
            </body>
        </html>
        """

        soup = BeautifulSoup(html, "lxml")

        base_url = "https://example.com/catalog/page1"

        links = parser.extract_links(soup, base_url)

        print("Результат конвертации ссылок:")

        for link in links:
            print(link)

        print("Относительные URL преобразованы в абсолютные")

    async def main():
        await parsing()
        await test_valid_html()
        await test_broken_html()
        await test_links_extraction()
        await test_relative_urls()

    asyncio.run(main())