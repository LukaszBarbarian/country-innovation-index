# src/ingestion/api_clients/base_api_client.py

from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, List, AsyncGenerator
import httpx

from src.bronze.models.manifest import BronzeManifestSource
from src.common.models.base_context import ContextBase
from src.common.models.raw_data import RawData

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
        self.base_url = self.config.get(self.base_url_setting_name)

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
    async def fetch_all(self, manifest_source: BronzeManifestSource) -> List[RawData]:
        raise NotImplementedError