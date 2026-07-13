import asyncio
from urllib.parse import urlparse
class CrawlerQueue:
    def __init__(self):
        
        self.queue = []
        self.added_urls = set()
        self.processed_urls = set()
        self.failed_urls = {}
        self.url_depths = {}
        self.total_added = 0
    
    def add_url(self, url: str, priority: int = 0):
        
        if not url:
            return False

        if url in self.added_urls:
            return False

        if url in self.processed_urls:
            return False

        if url in self.failed_urls:
            return False

        item = {
            "url": url,
            "priority": priority
        }

        self.queue.append(item)
        self.added_urls.add(url)
        self.total_added += 1

        self.queue.sort(key=lambda x: x["priority"], reverse=True)

        return True

    async def get_next(self) -> str:
        
        if len(self.queue) == 0:
            return None

        item = self.queue.pop(0)
        return item["url"]
    
    def set_depth(self, url: str, depth: int):
        
        self.url_depths[url] = depth

    def get_depth(self, url: str):
        
        return self.url_depths.get(url, 0)

    def mark_processed(self, url: str):
        
        self.processed_urls.add(url)

    def mark_failed(self, url: str, error: str):
        
        self.failed_urls [url]= error
    
    def get_stats(self) -> dict: 
        
        return {
            "queued": len(self.queue),
            "processed": len(self.processed_urls),
            "failed": len(self.failed_urls),
            "total_added": self.total_added
        }
    
class SemaphoreManager:
    
    def __init__(self, max_concurrent: int = 10, max_per_domain: int = 2):
        self.global_semaphore = asyncio.Semaphore(max_concurrent)
        self.max_per_domain = max_per_domain
        self.domain_semaphores = {}
        self.active_tasks = 0

    def _get_domain(self, url: str):
        
        parsed_url = urlparse(url)
        return parsed_url.netloc    
    
    def get_domain_semaphore(self, url: str):
        
        domain = self._get_domain(url)

        if domain not in self.domain_semaphores:
            self.domain_semaphores[domain] = asyncio.Semaphore(self.max_per_domain)

        return self.domain_semaphores[domain]
    
    async def acquire(self, url: str):
        
        domain_semaphore = self.get_domain_semaphore(url)

        await domain_semaphore.acquire()

        try:
            await self.global_semaphore.acquire()
        
        except Exception:
            domain_semaphore.release()
            raise

        self.active_tasks += 1
    
    def release(self, url: str):
        
        domain_semaphore = self.get_domain_semaphore(url)

        self.global_semaphore.release()
        domain_semaphore.release()

        self.active_tasks -= 1

    def get_semaphore_stats(self):
        
        return {
        "active_tasks": self.active_tasks,
        "domains": len(self.domain_semaphores)
        }
    