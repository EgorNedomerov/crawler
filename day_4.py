import asyncio
import aiohttp
import time
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
from contextvars import ContextVar
import logging 
logger = logging.getLogger(__name__)

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
    
    def __init__(
        self,
        session = None,
        rate_limiter = None,
        semaphore_manager = None,
        user_agent: str = "*",
        timeout = None
    ):
       
        self.robots_cache = {}
        self.current_base_url = ContextVar("current_base_url", default=None)
        self.blocked_urls = []

        self.robots_locks = {}
        self.lock_creation_lock = asyncio.Lock()

        self.session = session
        self.rate_limiter = rate_limiter
        self.semaphore_manager = semaphore_manager
        self.user_agent = user_agent
        self.timeout = timeout
    
    async def get_robots_lock(self, base_url: str):
    
        async with self.lock_creation_lock:
            if base_url not in self.robots_locks:
                self.robots_locks[base_url] = asyncio.Lock()

            return self.robots_locks[base_url]

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
        
        robots_lock = await self.get_robots_lock(base_url)

        async with robots_lock:
            if base_url in self.robots_cache:
                return{
                    "base_url": base_url,
                    "robots_url": robots_url,
                    "cached": True,
                    "success": True
                }

            parser = RobotFileParser()
            parser.set_url(robots_url)
            
            acquired = False

            try:
                parsed = urlparse(base_url)
                domain = parsed.netloc

                if self.semaphore_manager is not None:
                    await self.semaphore_manager.acquire(robots_url)
                    acquired = True
                
                if self.rate_limiter is not None:
                    await self.rate_limiter.acquire(domain)
                
                headers = { "User-Agent": self.user_agent}

                timeout = self.timeout
                
                if timeout is None:
                    timeout = aiohttp.ClientTimeout(total = 10)

                if self.session is not None:
                        async with self.session.get(robots_url, headers=headers, timeout=timeout) as response:
                        
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

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(robots_url, headers=headers) as response:
                        if response.status >= 400:
                            parser.parse ([])
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
                logger.warning("robots.txt не загружен для %s. Ошибка: %s", base_url, e)

                parser.parse([])
                self.robots_cache[base_url] = parser

                return {
                    "base_url": base_url,
                    "robots_url": robots_url,
                    "cached": False,
                    "success": False,
                    "error": str(e)
                }
            finally:
                if acquired:
                    self.semaphore_manager.release(robots_url)

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
            logger.warning("URL заблокирован robots.txt: %s", url)

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

