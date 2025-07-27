# src/common/registry/base_registry.py

from typing import Dict, Type, TypeVar, Generic

T = TypeVar("T")      # typ klasy rejestrowanej
K = TypeVar("K")      # typ klucza rejestracyjnego (np. enum, str)

class BaseRegistry(Generic[K, T]):
    _registry: Dict[K, Type[T]] = {}

    @classmethod
    def register(cls, key: K):
        def decorator(target_cls: Type[T]):
            if key in cls._registry:
                raise ValueError(f"{cls.__name__}: Key '{key}' is already registered with {cls._registry[key].__name__}")
            cls._registry[key] = target_cls
            return target_cls
        return decorator

    @classmethod
    def get_class(cls, domain_source):
        if domain_source not in cls._registry:
            raise ValueError("Nieznane źródło")
        return cls._registry[domain_source]