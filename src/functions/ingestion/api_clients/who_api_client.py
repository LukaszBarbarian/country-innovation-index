# src/ingestion/api_clients/who_api_client.py
import requests
from .base_api_client import ApiClient
from typing import Dict, Any

class WhoApiClient(ApiClient):
    def __init__(self, config: Any):
        super().__init__(config)
        self.base_url = self.config.get_setting(self.base_url_setting_name)

    async def fetch_data(self, **kwargs) -> Any:
        # Przykładowa implementacja dla WHO
        # URL i parametry będą zależne od konkretnego API WHO
        url = f"{self.base_url}/data"
        params = kwargs.get("params", {})
        
        # Możesz dodać retry logic here, np. z 'tenacity' lub własną implementację
        response = requests.get(url, params=params)
        response.raise_for_status() # Wyrzuci wyjątek dla statusów 4xx/5xx
        return response

    @property
    def default_file_format(self) -> str:
        return "json"

    @property
    def base_url_setting_name(self) -> str:
        return "WHO_API_BASE_URL" # Nazwa ustawienia w local.settings.json / App Settings