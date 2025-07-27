# src/ingestion/api_clients/api_client_factory.py

from typing import Any
from .api_client_registry import ApiClientRegistry
from src.common.enums.domain_source import DomainSource
from .base_api_client import ApiClient
from src.common.factory.base_factory import BaseFactoryFromRegistry
from src.common.registery.base_registry import BaseRegistry

class ApiClientFactory(BaseFactoryFromRegistry[DomainSource, ApiClient]):
    def get_registry(cls) -> BaseRegistry:
        return ApiClientRegistry