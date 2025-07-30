# src/ingestion/api_clients/api_client_registry.py

from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from typing import Any, Dict, Type
from src.common.api_clients.base_api_client import ApiClient

class ApiClientRegistry(BaseRegistry[DomainSource, Any]):
    _registry: Dict[DomainSource, Type[ApiClient]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[ApiClient]]:
        return cls._registry
