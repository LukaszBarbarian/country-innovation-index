# src/common/utils/decorator_duration.py

import datetime
from functools import wraps
from typing import Callable, Awaitable, Any
from dataclasses import replace

def track_duration(func: Callable) -> Callable:
    """
    Dekorator asynchroniczny, który mierzy czas wykonania udekorowanej funkcji.
    Tworzy nową kopię obiektu z zaktualizowanym czasem trwania.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Awaitable[Any]:
        start_time = datetime.datetime.utcnow()
        try:
            result = await func(*args, **kwargs)
            end_time = datetime.datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # Tworzymy nową instancję z nowym czasem trwania
            if hasattr(result, 'duration_in_ms'):
                result = replace(result, duration_in_ms=duration_ms)
            
            return result
        except Exception as e:
            end_time = datetime.datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # W przypadku błędu, Twój `ingest` musi zwrócić
            # IngestionResult z odpowiednimi danymi.
            raise e
            
    return wrapper