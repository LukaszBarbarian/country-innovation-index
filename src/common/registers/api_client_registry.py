# src/ingestion/api_clients/api_client_registry.py

from src.common.clients.api_clients.base_api_client import ApiClient
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from typing import Any, Dict, Type


class ApiClientRegistry(BaseRegistry[DomainSource, Any]):
    """
    A registry for mapping domain sources to their corresponding API client classes.
    This class inherits from `BaseRegistry` to provide a consistent way of managing
    and retrieving registered API clients.
    """
    _registry: Dict[DomainSource, Type[ApiClient]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[ApiClient]]:
        """
        Returns the internal dictionary that stores the registered API client classes.
        This method is required by the base class to access the registry data.
        """
        return cls._registry