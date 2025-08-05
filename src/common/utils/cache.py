# src/common/utils/cache.py
from typing import Any, Dict

class Cache:
    """
    Generyczna klasa do zarządzania pamięcią podręczną.
    """
    def __init__(self):
        self._store: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        """
        Pobiera wartość z cache'u na podstawie klucza.
        """
        return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        """
        Zapisuje wartość w cache'u pod danym kluczem.
        """
        self._store[key] = value

    def exists(self, key: str) -> bool:
        """
        Sprawdza, czy klucz istnieje w cache'u.
        """
        return key in self._store

    def clear(self) -> None:
        """
        Czyści cały cache.
        """
        self._store.clear()