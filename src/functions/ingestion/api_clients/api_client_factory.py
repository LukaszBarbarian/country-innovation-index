# src/ingestion/api_clients/api_client_factory.py
from typing import Type, Dict, Any
from .base_api_client import ApiClient
from .who_api_client import WhoApiClient
# from .worldbank_api_client import WorldBankApiClient # Jeśli dodasz

class ApiClientFactory:
    _clients: Dict[str, Type[ApiClient]] = {
        "who": WhoApiClient,
        # "worldbank": WorldBankApiClient,
    }

    @staticmethod
    def get_client(api_name: str, config: Any) -> ApiClient:
        client_class = ApiClientFactory._clients.get(api_name.lower())
        if not client_class:
            raise ValueError(f"No API client found for '{api_name}'")
        return client_class(config)