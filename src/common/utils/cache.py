# src/common/utils/cache.py
from typing import Any, Dict

class Cache:
    """
    A generic class for managing an in-memory cache.

    This class provides basic functionalities to store, retrieve, check for the
    existence of, and clear cached data using a key-value store.
    """
    def __init__(self):
        """
        Initializes a new, empty cache.
        """
        self._store: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        """
        Retrieves a value from the cache based on its key.
        
        Args:
            key (str): The key of the item to retrieve.
        
        Returns:
            Any: The cached value, or None if the key does not exist.
        """
        return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        """
        Stores a value in the cache with the given key.
        
        Args:
            key (str): The key to associate with the value.
            value (Any): The value to store.
        """
        self._store[key] = value

    def exists(self, key: str) -> bool:
        """
        Checks if a key exists in the cache.
        
        Args:
            key (str): The key to check for.
        
        Returns:
            bool: True if the key exists, False otherwise.
        """
        return key in self._store

    def clear(self) -> None:
        """
        Clears all items from the cache.
        """
        self._store.clear()