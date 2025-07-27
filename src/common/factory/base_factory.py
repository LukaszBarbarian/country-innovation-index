# src/functions/common/factory/base_factory_from_registry.py

from typing import TypeVar, Generic, Any
from abc import ABC, abstractmethod

K = TypeVar("K")  # Typ klucza
T = TypeVar("T")  # Typ zwracanej instancji

class BaseFactoryFromRegistry(Generic[K, T], ABC):
    """
    Bazowa klasa dla fabryk, które korzystają z rejestru do pobierania klas.
    """

    @classmethod
    @abstractmethod
    def get_registry(cls):
        """
        Zwraca rejestr (słownik lub obiekt z metodą get_class)
        """
        raise NotImplementedError

    @classmethod
    def get_instance(cls, key: K, *args: Any, **kwargs: Any) -> T:
        registry = cls.get_registry()
        get_class = registry.get_class if hasattr(registry, "get_class") else registry.get
        klass = get_class(key)
        return klass(*args, **kwargs)
