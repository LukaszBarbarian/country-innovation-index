# src/ingestion/api_clients/api_client_registry.py

from src.common.registery.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from typing import Any

class ApiClientRegistry(BaseRegistry[DomainSource, Any]):
    pass
