# src/ingestion/api_clients/base_api_client.py
from abc import ABC, abstractmethod
from typing import Any, Dict

class ApiClient(ABC):
    def __init__(self, config: Any): # Możesz przekazać ConfigManager
        self.config = config

    @abstractmethod
    async def fetch_data(self, **kwargs) -> Any: # Zwraca surową odpowiedź (np. requests.Response)
        """Pobiera dane z API."""
        pass

    @property
    @abstractmethod
    def default_file_format(self) -> str:
        """Domyślny format pliku dla danych z tego API (np. 'json', 'csv')."""
        pass

    @property
    @abstractmethod
    def base_url_setting_name(self) -> str:
        """Nazwa ustawienia w konfiguracji dla bazowego URL tego API."""
        pass