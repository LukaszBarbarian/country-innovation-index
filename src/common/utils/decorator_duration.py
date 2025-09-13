# src/common/utils/decorator_duration.py

import datetime
from functools import wraps
from typing import Callable, Awaitable, Any
from dataclasses import replace

def track_duration(func: Callable) -> Callable:
    """
    An asynchronous decorator that measures the execution time of the decorated function.
    It creates a new copy of the object returned by the function, updating it with the
    calculated duration.
    
    This decorator is designed to work with dataclasses that have a `duration_in_ms` attribute.
    It uses `dataclasses.replace` to create a new instance with the updated value,
    ensuring immutability if the original object was a frozen dataclass.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Awaitable[Any]:
        start_time = datetime.datetime.utcnow()
        try:
            result = await func(*args, **kwargs)
            end_time = datetime.datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # Create a new instance with the new duration
            if hasattr(result, 'duration_in_ms'):
                result = replace(result, duration_in_ms=duration_ms)
            
            return result
        except Exception as e:
            end_time = datetime.datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # In case of an error, your `ingest` method must return
            # an IngestionResult with the appropriate data.
            raise e
            
    return wrapper