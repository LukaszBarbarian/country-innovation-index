import asyncio
import functools
import logging

logger = logging.getLogger(__name__)

def async_retry(max_retries: int = 3, delay: float = 2.0, exceptions: tuple = (Exception,)):
    """
    Async retry decorator for coroutine functions.

    Retries the decorated async function up to `max_retries` times if it raises
    any of the specified exceptions. Between retries, it waits for `delay` seconds.
    After exceeding the maximum attempts, the last exception is raised.

    Args:
        max_retries (int): Maximum number of retry attempts before giving up. Default is 3.
        delay (float): Delay in seconds between consecutive retries. Default is 2.0.
        exceptions (tuple): Tuple of exception classes that should trigger a retry. Default is (Exception,).

    Returns:
        Callable: A wrapped coroutine function with retry logic.

    Usage:
        @async_retry(max_retries=5, delay=1, exceptions=(httpx.HTTPStatusError,))
        async def fetch_data(...):
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            """
            Wrapper coroutine that performs the retry logic.

            Args:
                *args: Positional arguments passed to the decorated function.
                **kwargs: Keyword arguments passed to the decorated function.

            Returns:
                Any: The return value of the decorated coroutine if it succeeds.

            Raises:
                Exception: The last caught exception if all retry attempts fail.
            """
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    logger.warning(f"[Retry {attempt}/{max_retries}] {func.__name__} failed: {e}")
                    if attempt < max_retries:
                        await asyncio.sleep(delay)
            logger.error(f"Function {func.__name__} failed after {max_retries} attempts")
            raise last_exception

        return wrapper
    return decorator
