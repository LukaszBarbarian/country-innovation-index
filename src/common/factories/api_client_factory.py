# src/ingestion/api_clients/api_client_factory.py

from typing import Any
from src.common.clients.api_clients.base_api_client import ApiClient
from src.common.enums.domain_source import DomainSource
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.registers.api_client_registry import ApiClientRegistry
from src.common.registers.base_registry import BaseRegistry

class ApiClientFactory(BaseFactoryFromRegistry[DomainSource, ApiClient]):

    @classmethod
    def get_registry(cls) -> BaseRegistry:
        return ApiClientRegistry()