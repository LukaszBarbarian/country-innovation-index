# src/ingestion/api_clients/api_client_registry.py

from typing import Type, Dict, Callable, Any
import logging
from src.common.enums.domain_source import DomainSource

logger = logging.getLogger(__name__)

class ApiClientRegistry:
    """
    Rejestr dla klas klientów API. Pozwala na dynamiczne dodawanie i pobieranie
    klas klientów na podstawie ich unikalnych identyfikatorów (api_identifier).
    """
    _registered_clients: Dict[DomainSource, Type[Any]] = {} # Słownik przechowujący identyfikator -> klasa klienta

    @classmethod
    def register(cls, api_identifier: DomainSource) -> Callable:
        """
        Dekorator do rejestrowania klas klientów API.
        
        Args:
            api_identifier (str): Unikalny identyfikator klienta API (np. "who", "world_bank").
                                  Zostanie użyty do pobrania klienta z rejestru.
        
        Returns:
            Callable: Dekorator, który rejestruje klasę.
        """
        if not isinstance(api_identifier, DomainSource) or not api_identifier:
            raise ValueError("api_identifier musi być niepustym stringiem.")

        def decorator(cls_to_register: Type[Any]) -> Type[Any]:
            if api_identifier in cls._registered_clients:
                logger.warning(
                    f"Klient API '{api_identifier}' jest już zarejestrowany. "
                    f"Nadpisuję istniejącą rejestrację dla klasy: {cls_to_register.__name__}."
                )
            cls._registered_clients[api_identifier] = cls_to_register
            logger.info(f"Klient API '{cls_to_register.__name__}' zarejestrowany pod identyfikatorem: '{api_identifier}'.")
            return cls_to_register
        return decorator

    @classmethod
    def get_client_class(cls, api_identifier: DomainSource) -> Type[Any]:
        """
        Pobiera klasę klienta API z rejestru na podstawie jego identyfikatora.

        Args:
            api_identifier (str): Unikalny identyfikator klienta API.

        Returns:
            Type[Any]: Klasa klienta API.

        Raises:
            ValueError: Jeśli klient o podanym identyfikatorze nie jest zarejestrowany.
        """
        client_class = cls._registered_clients.get(api_identifier)
        if client_class is None:
            raise ValueError(f"Klient API o identyfikatorze '{api_identifier}' nie jest zarejestrowany.")
        return client_class

    @classmethod
    def list_registered_clients(cls) -> Dict[DomainSource, str]:
        """Zwraca słownik zarejestrowanych klientów (identyfikator -> nazwa klasy)."""
        return {id: cls_type.__name__ for id, cls_type in cls._registered_clients.items()}
