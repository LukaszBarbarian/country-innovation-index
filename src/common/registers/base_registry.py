# src/common/registry/base_registry.py

from typing import Dict, Type, TypeVar, Generic, Any
from abc import ABC, abstractmethod

T = TypeVar("T")      # typ klasy rejestrowanej
K = TypeVar("K")      # typ klucza rejestracyjnego (np. enum, str)

class BaseRegistry(Generic[K, T], ABC): # Dodaj ABC do dziedziczenia
    # USUWAMY _registry stąd! Będzie w klasach dziedziczących.

    @classmethod
    @abstractmethod # Teraz to musi być zaimplementowane w klasach dziedziczących
    def _get_registry_dict(cls) -> Dict[K, Type[T]]:
        """
        Zwraca unikalny słownik rejestru dla tej konkretnej podklasy rejestru.
        """
        pass # To będzie zaimplementowane w podklasach

    @classmethod
    def register(cls, key: K):
        def decorator(target_cls: Type[T]):
            registry_dict = cls._get_registry_dict() # Pobieramy unikalny słownik
            if key in registry_dict:
                raise ValueError(f"{cls.__name__}: Key '{key}' is already registered with {registry_dict[key].__name__}")
            registry_dict[key] = target_cls
            return target_cls
        return decorator

    @classmethod
    def get_class(cls, key: K) -> Type[T]: # Zmieniam domain_source na key, żeby było spójne z K
        registry_dict = cls._get_registry_dict() # Pobieramy unikalny słownik
        if key not in registry_dict:
            raise ValueError(f"{cls.__name__}: Key '{key}' - Nieznane źródło") # Lepsza wiadomość błędu
        return registry_dict[key]