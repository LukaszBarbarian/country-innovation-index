# src/common/utils/decorators.py
import datetime
from functools import wraps
from typing import Callable, Awaitable, Any

def track_duration(func: Callable) -> Callable:
    """
    Dekorator asynchroniczny, który mierzy czas wykonania udekorowanej funkcji.
    Dodaje atrybut `duration_in_ms` do zwracanego obiektu.
    Zakłada, że udekorowana funkcja zwraca obiekt, który ma atrybut `duration_in_ms`.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Awaitable[Any]:
        start_time = datetime.datetime.utcnow()
        try:
            result = await func(*args, **kwargs)
            end_time = datetime.datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # Weryfikacja i ustawienie atrybutu duration_in_ms
            if hasattr(result, 'duration_in_ms'):
                setattr(result, 'duration_in_ms', duration_ms)
            
            return result
        except Exception as e:
            end_time = datetime.datetime.utcnow()
            duration_ms = int((end_time - start_time).total_seconds() * 1000)
            
            # Jeśli funkcja rzuciła błąd, musimy go obsłużyć i nadal zwrócić IngestionResult
            # W tym przypadku, nie możemy po prostu dodać atrybutu, bo wynik nie został zwrócony.
            # Musimy zmodyfikować logikę w metodzie, aby przechwytywała błąd.
            raise e
            
    return wrapper