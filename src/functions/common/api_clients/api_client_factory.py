# src/ingestion/api_clients/api_client_factory.py
from typing import Type, Dict, Any
from .base_api_client import ApiClient
from ...ingestion.api_clients.who_api_client import WhoApiClient
from src.common.enums.domain_source import DomainSource
# from .worldbank_api_client import WorldBankApiClient # Jeśli dodasz

class ApiClientFactory:
    _clients: Dict[DomainSource, Type[ApiClient]] = {
       DomainSource.WHO : WhoApiClient,
    }

    @staticmethod
    def get_client(api_name: DomainSource, config: Any) -> ApiClient:
        client_class = ApiClientFactory._clients.get(api_name)
        if not client_class:
            raise ValueError(f"No API client found for '{api_name}'")
        return client_class(config)