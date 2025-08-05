# src/ingestion/api_clients/base_api_client.py

from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, List, AsyncGenerator
import httpx

from src.common.models.api_result import ApiResult

logger = logging.getLogger(__name__)

class ApiClient(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich klientów API.
    Definiuje podstawowy interfejs dla pobierania danych.
    """
    # Usunięto api_identifier z konstruktora
    def __init__(self, config: Any, base_url_setting_name: str): 
        self.config = config
        self.base_url_setting_name = base_url_setting_name
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



    @abstractmethod
    async def fetch_all(self, payload: Dict[str, Any]) -> List[ApiResult]:
        """
        Główna metoda publiczna: zwraca generator wszystkich rekordów z API,
        obsługując paginację wewnętrznie, jeśli to konieczne dla danego API.
        """
        pass