import asyncio
import aiohttp
import time
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
from contextvars import ContextVar
class RateLimiter:
    
    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True ):
       
        if requests_per_second <=0:
            raise ValueError ("requests_per_second должен быть положительным")
        self.requests_per_second = requests_per_second
        self.per_domain = per_domain
        self.min_interval = 1 / requests_per_second
        self.last_request_time = {}
        self.global_lock = asyncio.Lock ()
        self.domain_locks = {}
        self.stats_lock = asyncio.Lock()
        self.total_wait_time = 0.0
        self.total_requests = 0
        self.wait_count = 0

    async def acquire(self, domain: str = None):
        
        if self.per_domain:
            key = domain or "unknown"
        
            if key not in self.domain_locks:
                self.domain_locks[key] = asyncio.Lock()

            lock = self.domain_locks[key]

        else:
            key = "global"
            lock = self.global_lock
        
        async with lock:
            now = time.perf_counter()
            last_time = self.last_request_time.get(key)
            
            if last_time is not None:
                elapsed = now - last_time
                wait_time = self.min_interval - elapsed
                
                if wait_time > 0:
                    async with self.stats_lock:
                        self.wait_count += 1
                        self.total_wait_time += wait_time

                    await asyncio.sleep(wait_time)
            
            self.last_request_time[key] = time.perf_counter()
            
            async with self.stats_lock:
                self.total_requests += 1
        
    def get_stats(self):
        
        if self.total_requests > 0:
            average_delay = self.total_wait_time / self.total_requests
        
        else:
            average_delay = 0
        return {
            "requests_per_second": self.requests_per_second,
            "per_domain": self.per_domain,
            "total_requests": self.total_requests,
            "wait_count": self.wait_count,
            "total_wait_time": round(self.total_wait_time, 2),
            "average_delay": round(average_delay, 2)
        }

class RobotsParser:
    
    def __init__(self):
       
        self.robots_cache = {}
        self.current_base_url = ContextVar("current_base_url", default=None)
        self.blocked_urls = []

    async def fetch_robots(self, base_url: str) -> dict:
        robots_url = urljoin(base_url, "/robots.txt")

        self.current_base_url.set(base_url)

        if base_url in self.robots_cache:
            return {
                "base_url": base_url,
                "robots_url": robots_url,
                "cached": True,
                "success": True
            }

        parser = RobotFileParser()
        parser.set_url(robots_url)

        try:
            timeout = aiohttp.ClientTimeout(total=10)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(robots_url) as response:

                    if response.status >= 400:
                        parser.parse([])
                        self.robots_cache[base_url] = parser

                        return {
                            "base_url": base_url,
                            "robots_url": robots_url,
                            "cached": False,
                            "success": False,
                            "status": response.status
                        }

                    text = await response.text()
                    parser.parse(text.splitlines())

                    self.robots_cache[base_url] = parser
            
                    return {
                        "base_url": base_url,
                        "robots_url": robots_url,
                        "cached": False,
                        "success": True,
                        "status": response.status
                    }

        except Exception as e:
            print(f"Предупреждение: robots.txt не загружен для {base_url}. Ошибка: {e}")

            parser.parse([])
            self.robots_cache[base_url] = parser

            return {
                "base_url": base_url,
                "robots_url": robots_url,
                "cached": False,
                "success": False,
                "error": str(e)
            }

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        self.current_base_url.set(base_url)

        parser = self.robots_cache.get(base_url)

        if parser is None:
            return True

        allowed = parser.can_fetch(user_agent, url)

        if not allowed:
            self.blocked_urls.append(url)

        return allowed

    def get_crawl_delay(self, user_agent: str = "*") -> float:
        
        base_url = self.current_base_url.get()

        if base_url is None:
            return 0.0

        parser = self.robots_cache.get(base_url)

        if parser is None:
            return 0.0

        delay = parser.crawl_delay(user_agent)

        if delay is None:
            return 0.0

        return float(delay)

    def get_stats(self):
        
        return {
            "robots_cached_domains": len(self.robots_cache),
            "blocked_by_robots": len(self.blocked_urls),
            "blocked_urls": self.blocked_urls
        }

