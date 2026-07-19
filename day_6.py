import json
import csv
import io
from abc import ABC, abstractmethod
from datetime import datetime
import aiofiles
import aiosqlite

STORAGE_FIELDS = [
    "url",
    "title",
    "text",
    "links",
    "metadata",
    "crawled_at",
    "status_code",
    "content_type"
]

def normalize_data(data:dict)-> dict:
    
    normalized = {}
     
    for field in STORAGE_FIELDS:
        value = data.get(field)

        if field == "links":
            if value is None:
                value = []
            elif not isinstance(value, list):
                value = [str(value)]

        elif field == "metadata":
            if value is None:
                value = {}
            elif not isinstance(value, dict):
                value = {"value": str(value)}

        elif field == "crawled_at":
            if value is None:
                value = datetime.now().isoformat()
            elif isinstance(value, datetime):
                value = value.isoformat()

        elif field == "status_code":
            if value is None:
                value = 0

        elif value is None:
            value = ""

        normalized[field] = value

    return normalized

def prepare_for_json(data: dict) -> dict:
    
    normalized = normalize_data(data)

    result = {}

    for key, value in normalized.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()
        else:
            result[key] = value

    return result

def prepare_for_csv(data: dict) -> dict:
    
    normalized = normalize_data(data)

    result = {}

    for key, value in normalized.items():
        if isinstance(value, list) or isinstance(value, dict):
            result[key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
        else:
            result[key] = value

    return result

def prepare_for_sqlite(data: dict) -> tuple:
    
    normalized = normalize_data(data)

    return (
        normalized["url"],
        normalized["title"],
        normalized["text"],
        json.dumps(normalized["links"], ensure_ascii=False),
        json.dumps(normalized["metadata"], ensure_ascii=False),
        normalized["crawled_at"],
        normalized["status_code"],
        normalized["content_type"]
    )

class DataStorage(ABC):
    
    @abstractmethod
    async def save(self, data: dict):
        pass

    @abstractmethod
    async def close(self):
        pass

class JSONStorage(DataStorage):

    def __init__(
        self,
        filename: str,
        encoding: str = "utf-8",
        indent: int | None = None,
        mode: str = "jsonl"
    ):
        if mode not in ["jsonl", "json_array"]:
            raise ValueError("mode должен быть 'jsonl' или 'json_array'")
        self.filename = filename
        self.encoding = encoding
        self.indent = indent
        self.saved_count = 0
        self.closed = False
        self.mode = mode
        self.initialized = False
        self.first_item = True

    async def save(self, data: dict):
        
        if self.closed:
            raise OSError("JSONStorage уже закрыт")

        prepared_data = prepare_for_json(data)

        if self.mode == "jsonl":
            json_line = json.dumps(
                prepared_data,
                ensure_ascii=False
            )
            async with aiofiles.open(self.filename,mode = "a", encoding = self.encoding) as file:
                await file.write(json_line)
                await file.write("\n")

        else:
            if not self.initialized:
                async with aiofiles.open(self.filename, mode = "w", encoding = self.encoding) as file:
                    await file.write("[\n")

                self.initialized = True
            
            json_object = json.dumps(prepared_data, ensure_ascii = False, indent = self.indent)

            async with aiofiles.open(self.filename, mode = "a", encoding = self.encoding) as file:
                if not self.first_item:
                    await file.write(",\n")

                await file.write(json_object)
            
            self.first_item = False
            
        self.saved_count += 1

    async def close(self):
        
        if self.closed:
            return
        
        if self.mode == "json_array":
            if not self.initialized:
                async with aiofiles.open(
                    self.filename,
                    mode="w",
                    encoding=self.encoding
                ) as file:
                    await file.write("[\n")

                self.initialized = True

            async with aiofiles.open(
                self.filename,
                mode="a",
                encoding=self.encoding
            ) as file:
                await file.write("\n]\n")

        self.closed = True

class CSVStorage(DataStorage):

    def __init__(
        self,
        filename: str,
        encoding: str = "utf-8"
    ):
        self.filename = filename
        self.encoding = encoding
        self.fieldnames = None
        self.header_written = False
        self.saved_count = 0
        self.closed = False

    async def save(self, data: dict):
        
        if self.closed:
            raise OSError("CSVStorage уже закрыт")

        prepared_data = prepare_for_csv(data)

        if self.fieldnames is None:
            self.fieldnames = list(prepared_data.keys())

        output = io.StringIO()

        writer = csv.DictWriter(
            output,
            fieldnames=self.fieldnames,
            extrasaction="ignore"
        )

        if not self.header_written:
            writer.writeheader()
            self.header_written = True

        writer.writerow(prepared_data)

        async with aiofiles.open(
            self.filename,
            mode="a",
            encoding=self.encoding,
            newline=""
        ) as file:
            await file.write(output.getvalue())

        self.saved_count += 1

    async def close(self):
        
        self.closed = True

class SQLiteStorage(DataStorage):

    def __init__(
        self,
        filename: str,
        batch_size: int = 10
    ):
        self.filename = filename
        self.batch_size = batch_size
        self.connection = None
        self.saved_count = 0
        self.closed = False

    async def init_db(self):
        
        self.connection = await aiosqlite.connect(self.filename)

        await self.connection.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                title TEXT,
                text TEXT,
                links TEXT,
                metadata TEXT,
                crawled_at TEXT,
                status_code INTEGER,
                content_type TEXT
            )
        """)

        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_pages_url
            ON pages(url)
        """)

        await self.connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_pages_title
            ON pages(title)
        """)

        await self.connection.commit()

    async def save(self, data: dict):
        
        if self.closed:
            raise OSError("SQLiteStorage уже закрыт")

        if self.connection is None:
            await self.init_db()

        row = prepare_for_sqlite(data)
        
        await self.connection.execute("""
            INSERT OR REPLACE INTO pages (
                url,
                title,
                text,
                links,
                metadata,
                crawled_at,
                status_code,
                content_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, row)

        await self.connection.commit()

        self.saved_count += 1
           
    async def save_many(self, items: list[dict]):
        
        if self.closed:
            raise OSError("SQLiteStorage уже закрыт")

        if self.connection is None:
            await self.init_db()

        if not items:
            return

        rows = [prepare_for_sqlite(item) for item in items]

        for index in range(0, len(rows), self.batch_size):
            batch = rows[index:index + self.batch_size]

            await self.connection.executemany("""
                INSERT OR REPLACE INTO pages (
                    url,
                    title,
                    text,
                    links,
                    metadata,
                    crawled_at,
                    status_code,
                    content_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)

            await self.connection.commit()

            self.saved_count += len(batch)

    async def read_all(self) -> list[dict]:
        if self.connection is None:
            await self.init_db()

        cursor = await self.connection.execute("""
            SELECT
                url,
                title,
                text,
                links,
                metadata,
                crawled_at,
                status_code,
                content_type
            FROM pages
        """)

        rows = await cursor.fetchall()
        await cursor.close()

        results = []

        for row in rows:
            results.append({
                "url": row[0],
                "title": row[1],
                "text": row[2],
                "links": json.loads(row[3]) if row[3] else [],
                "metadata": json.loads(row[4]) if row[4] else {},
                "crawled_at": row[5],
                "status_code": row[6],
                "content_type": row[7]
            })

        return results

    async def close(self):
        if self.closed:
            return
        
        if self.connection is not None:
            await self.connection.close()

        self.closed = True