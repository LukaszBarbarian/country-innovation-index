# src/ingestion/api_clients/base_api_client.py

from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, List, AsyncGenerator
import httpx

logger = logging.getLogger(__name__)

class ApiClient(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich klientów API.
    Definiuje podstawowy interfejs dla pobierania danych.
    """
    # Usunięto api_identifier z konstruktora
    def __init__(self, config: Any): 
        self.config = config
        self.base_url = self.config.get_setting(self.base_url_setting_name)
        if not self.base_url:
            logger.error(f"Ustawienie URL bazowego '{self.base_url_setting_name}' nie zostało znalezione w konfiguracji.")
            raise ValueError(f"Brak wymaganego ustawienia: '{self.base_url_setting_name}'")
        logger.debug(f"ApiClient initialized with base_url: {self.base_url}")
        
        self.client = httpx.AsyncClient() 

    async def __aenter__(self):
        """Metoda do użycia z 'async with', inicjalizuje sesję klienta HTTP."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Metoda do użycia z 'async with', zamyka sesję klienta HTTP."""
        await self.client.aclose()


    @property
    @abstractmethod
    def base_url_setting_name(self) -> str:
        """
        Zwraca nazwę ustawienia w ConfigManagerze, które zawiera bazowy URL dla tego API.
        """
        pass

    @abstractmethod
    async def _fetch_single_page(self, params: Dict[str, Any]) -> httpx.Response:
        """
        Pobiera pojedynczą stronę danych. Prywatna, używana wewnętrznie przez implementacje `fetch_all_records`.
        """
        pass

    @abstractmethod
    def _extract_records_from_response(self, response_json: Any) -> List[Any]:
        """
        Wyodrębnia listę rekordów z surowej odpowiedzi JSON.
        """
        pass

    @abstractmethod
    async def fetch_all_records(self, initial_request_payload: Dict[str, Any]) -> AsyncGenerator[Any, None]:
        """
        Główna metoda publiczna: zwraca generator wszystkich rekordów z API,
        obsługując paginację wewnętrznie, jeśli to konieczne dla danego API.
        """
        pass