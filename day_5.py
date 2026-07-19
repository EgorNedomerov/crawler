import asyncio
import logging 
logger = logging.getLogger(__name__)

class RetryStrategy:
    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0, retry_on: list = None):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        if retry_on is None:
            self.retry_on = [TransientError, NetworkError]
        else:
            self.retry_on = retry_on

        self.error_counts = {}
        self.successful_retries = 0
        self.retry_times = []            
        self.permanent_error_urls = []
        self.attempt_logs = []
        self.current_attempt = 1

    async def execute_with_retry(self, coro, *args, **kwargs):
        
        last_error = None
        
        for attempt in range(1, self.max_retries +2):
            try:
                self.current_attempt = attempt 
                result = await coro(*args, **kwargs)

                if attempt > 1:
                    self.successful_retries += 1
                    logger.info("Успешная повторная попытка: попытка %s", attempt)

                self.attempt_logs.append ({
                    "attempt": attempt,
                    "status": "success",
                    "error_type": None,
                    "url": "",
                    "delay": 0
                })

                return result
            
            except Exception as error:
                last_error = error

                error_type = type(error).__name__
                if error_type not in self.error_counts:
                    self.error_counts[error_type] = 0
                
                self.error_counts[error_type] += 1

                url = getattr(error, "url", "")

                if isinstance(error, PermanentError):
                    if url:
                        self.permanent_error_urls.append(url)
                
                should_retry = False

                for error_class in self.retry_on:
                    if isinstance(error, error_class):
                        should_retry = True
                        break
                
                has_attempts_left = attempt <= self.max_retries

                if not should_retry or not has_attempts_left:
                    self.attempt_logs.append ({
                        "attempt": attempt,
                        "status": "failed",
                        "error_type": error_type,
                        "url": url,
                        "delay": 0
                    })

                    logger.error(
                        "Ошибка без повтора. Тип: %s. URL: %s. Попытка: %s",
                        error_type,
                        url,
                        attempt
                    )
                    raise error
                
                if isinstance(error, NetworkError):
                    delay = self.backoff_factor ** attempt
                elif isinstance(error, TransientError):
                    delay = self.backoff_factor ** (attempt - 1)
                else:
                    delay = self.backoff_factor ** (attempt - 1)
                
                delay = min(delay, 30) 

                self.retry_times.append(delay)
                self.attempt_logs.append({
                    "attempt": attempt,
                    "status": "retry",
                    "error_type": error_type,
                    "url": url,
                    "delay": delay
                })

                logger.warning(
                    "Ошибка: %s. URL: %s. Попытка: %s. Следующая попытка: %.2f сек.", 
                    error_type,
                    url,
                    attempt,
                    delay
                )

                await asyncio.sleep(delay)
        
        raise last_error
    
    def get_stats(self):
        
        if self.retry_times:
            average_retry_time = sum(self.retry_times) / len(self.retry_times)
        else:
            average_retry_time = 0

        return {
            "error_counts": self.error_counts,
            "successful_retries": self.successful_retries,
            "average_retry_time": round(average_retry_time, 2),
            "permanent_error_urls": self.permanent_error_urls,
            "attempt_logs": self.attempt_logs,
            "current_attempt": self.current_attempt
        }
            
class TransientError(Exception):
    def __init__(self, message: str, url: str = "", status: int | None = None):
        super().__init__(message)
        self.message = message
        self.url = url
        self.status = status

class NetworkError(Exception):
    def __init__(self, message: str, url: str = "", status: int | None = None ):
        super().__init__(message)
        self.message = message
        self.url = url
        self.status = status

class PermanentError(Exception):
    def __init__(self, message: str, url: str = "", status: int | None = None ):
        super().__init__(message)
        self.message = message
        self.url = url
        self.status = status

class ParseError(Exception):
    def __init__(self, message: str, url: str = "", status: int | None = None ):
        super().__init__(message)
        self.message = message
        self.url = url
        self.status = status
