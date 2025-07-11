# functions/api_factory/factory.py
from typing import Type, Dict
from ..api_client.base import ApiClient
from ...shared.enums.api_type import ApiType
from typing import Type, TypeVar, Dict


T = TypeVar('T', bound='ApiClient')

class ApiFactory:
    _api_clients: Dict[ApiType, Type['ApiClient']] = {}

    @classmethod
    def register_api_client(cls, api_type: ApiType):
        """
        
        """
        def decorator(client_class: T) -> T:
            if not issubclass(client_class, ApiClient):
                raise TypeError("Registered class must be a subclass of ApiClient.")
            if api_type in cls._api_clients:
                raise ValueError(f"API type '{api_type.value}' already registered.")
            cls._api_clients[api_type] = client_class
            return client_class
        return decorator
    
    

    @classmethod
    def get_api_client(cls, api_type: ApiType) -> 'ApiClient':
        """
        
        """
        client_class = cls._api_clients.get(api_type)
        if not client_class:
            raise ValueError(f"No client registered for API type: {api_type.value}. "
                             f"Ensure the API module is imported or the API type is correctly registered.")
        return client_class() 