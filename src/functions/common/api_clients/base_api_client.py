# src/ingestion/api_clients/base_api_client.py

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging
from src.common.enums.domain_source import DomainSource

logger = logging.getLogger(__name__)

class ApiClient(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich klientów API.
    Definiuje podstawowy interfejs dla pobierania danych.
    """
    def __init__(self, config: Any): # config powinien być ConfigManagerem
        self.config = config
        self.base_url = self.config.get_setting(self.base_url_setting_name)
        if not self.base_url:
            logger.error(f"Ustawienie URL bazowego '{self.base_url_setting_name}' nie zostało znalezione w konfiguracji.")
            raise ValueError(f"Brak wymaganego ustawienia: '{self.base_url_setting_name}'")
        logger.debug(f"ApiClient initialized with base_url: {self.base_url}")

    @property
    @abstractmethod
    def api_identifier(self) -> DomainSource:
        """
        Zwraca unikalny identyfikator dla tego klienta API (np. "who", "world_bank").
        Używany przez rejestr klientów API.
        """
        pass

    @property
    @abstractmethod
    def base_url_setting_name(self) -> str:
        """
        Zwraca nazwę ustawienia w ConfigManagerze, które zawiera bazowy URL dla tego API.
        """
        pass

    @property
    @abstractmethod
    def default_file_format(self) -> str:
        """
        Zwraca domyślny format pliku, w jakim dane z tego API powinny być zapisywane
        w warstwie Bronze (np. "json", "csv", "xml").
        """
        pass

    @abstractmethod
    async def fetch_data(self, api_request_payload: Dict[str, Any]) -> Any:
        """
        Abstrakcyjna metoda do pobierania danych z API.
        
        Args:
            api_request_payload (Dict[str, Any]): Słownik zawierający szczegóły żądania API
                                                    (np. endpoint_path, query_params).
        Returns:
            Any: Surowa odpowiedź z API (np. obiekt requests.Response, string JSON/CSV).
        """
        pass