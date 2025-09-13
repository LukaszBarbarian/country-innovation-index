# src/ingestion/api_clients/base_api_client.py

from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, List, AsyncGenerator
import httpx

from src.bronze.models.manifest import BronzeManifestSource
from src.common.models.base_context import ContextBase
from src.common.models.raw_data import RawData

logger = logging.getLogger(__name__)

class ApiClient(ABC):
    """
    An abstract base class for all API clients.
    It defines the fundamental interface for fetching data from an external API.
    """
    # Removed api_identifier from the constructor
    def __init__(self, config: Any, base_url_setting_name: str):
        """
        Initializes the ApiClient with configuration and a base URL setting name.

        Args:
            config (Any): The configuration object, which should be able to retrieve settings.
            base_url_setting_name (str): The name of the configuration key that holds the base URL for the API.
        
        Raises:
            ValueError: If the required base URL setting is not found in the configuration.
        """
        self.config = config
        self.base_url_setting_name = base_url_setting_name
        self.base_url = self.config.get(self.base_url_setting_name)

        if not self.base_url:
            logger.error(f"Base URL setting '{self.base_url_setting_name}' was not found in the configuration.")
            raise ValueError(f"Missing required setting: '{self.base_url_setting_name}'")
        logger.debug(f"ApiClient initialized with base_url: {self.base_url}")

        self.client = httpx.AsyncClient()

    async def __aenter__(self):
        """
        An asynchronous context manager method.
        It initializes and returns the API client instance for use within an `async with` statement.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        An asynchronous context manager method.
        It ensures the `httpx.AsyncClient` session is properly closed when the `async with` block is exited.
        """
        await self.client.aclose()


    @abstractmethod
    async def fetch_all(self, manifest_source: BronzeManifestSource) -> List[RawData]:
        """
        An abstract method that must be implemented by all subclasses.
        It defines the contract for fetching all data from an API source.

        Args:
            manifest_source (BronzeManifestSource): A manifest object containing details
                                                    about the specific data source to fetch.

        Returns:
            List[RawData]: A list of `RawData` objects, each containing fetched data.

        Raises:
            NotImplementedError: This exception is raised by default and must be handled
                                 by the subclass implementation.
        """
        raise NotImplementedError