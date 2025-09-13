# src/common/storage/base_azure_client_manager.py

from abc import ABC, abstractmethod
import logging
from typing import Optional, TypeVar, Generic

from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError
from src.common.config.config_manager import ConfigManager

logger = logging.getLogger(__name__)

# Types for generic clients
TServiceClient = TypeVar('TServiceClient')
TClient = TypeVar('TClient')

class AzureClientManagerBase(ABC, Generic[TServiceClient, TClient]):
    """
    An abstract base class for all Azure SDK client managers (e.g., Blob, Queue).
    It is responsible for initializing connections to a specific Azure service.
    Handles authentication using either a connection string or Azure Identity
    (Managed Identity/Service Principal).
    """

    _config: ConfigManager = ConfigManager()

    def __init__(self, resource_name: str, storage_account_name_setting_name: str, base_url_suffix: str):
        """
        Initializes the base client manager.

        Args:
            resource_name (str): The name of the specific resource (e.g., container or queue).
            storage_account_name_setting_name (str): The name of the configuration setting
                                                      for the storage account name.
            base_url_suffix (str): The URL suffix for the Azure service (e.g., ".blob.core.windows.net").

        Raises:
            ValueError: If any of the required parameters are empty.
        """
        if not resource_name:
            raise ValueError("Resource name cannot be empty.")
        if not storage_account_name_setting_name:
            raise ValueError("Connection string setting name cannot be empty.")
        if not base_url_suffix:
            raise ValueError("URL suffix cannot be empty.")

        self._resource_name: str = resource_name
        self._storage_account_name_setting_name: str = storage_account_name_setting_name
        self._base_url_suffix: str = base_url_suffix

        self._service_client: Optional[TServiceClient] = None
        self._client: Optional[TClient] = None

        self._initialize_clients()
        logger.info(f"AzureClientManagerBase initialized for resource '{self.resource_name}'.")


    @abstractmethod
    def _create_service_client_from_identity(self, account_url: str, credential) -> TServiceClient:
        """
        An abstract method to create a `ServiceClient` using Azure Identity.
        This method must be implemented by subclasses to be specific to the Azure service.
        """
        pass

    @abstractmethod
    def _get_resource_client(self, service_client: TServiceClient, resource_name: str) -> TClient:
        """
        An abstract method to get a client for a specific resource (container/queue).
        Subclasses must implement this to return a client specific to their resource type.
        """
        pass

    def _initialize_clients(self):
        """
        Initializes the `ServiceClient` and the specific resource client.

        This method attempts to authenticate using Azure Identity (Managed Identity/Service Principal).
        It's designed to be robust and will raise a `RuntimeError` if authentication fails.
        """
        storage_account_name = self._config.get(self._storage_account_name_setting_name)
        
        if not self._service_client and storage_account_name:
            logger.info(f"Attempting to connect to Azure using Azure Identity for account: {storage_account_name}.")
            try:
                credential = DefaultAzureCredential()
                account_url = f"https://{storage_account_name}{self._base_url_suffix}"
                self._account_url = account_url

                self._service_client = self._create_service_client_from_identity(account_url, credential)
                logger.info(f"Connection using Azure Identity for account {storage_account_name} successful.")
            except Exception as e:
                logger.error(f"Error creating ServiceClient with Azure Identity for account {storage_account_name}: {e}")
                raise RuntimeError(f"Failed to initialize Azure connection: {e}")

        if not self._service_client:
            raise RuntimeError("Could not initialize ServiceClient. No valid storage account name or credentials found.")

        self._client = self._get_resource_client(self._service_client, self.resource_name)

    @property
    def resource_name(self) -> str:
        """
        Returns the name of the resource (container/queue) the manager is working with.
        """
        return self._resource_name

    @property
    def service_client(self) -> TServiceClient:
        """
        Returns the initialized service client.
        Lazily re-initializes the client if it's found to be empty.
        """
        if not self._service_client:
            logger.warning("ServiceClient was empty, attempting re-initialization.")
            self._initialize_clients()
        assert self._service_client is not None, "ServiceClient should be initialized"
        return self._service_client


    @property
    def client(self) -> TClient:
        """
        Returns the initialized resource client.
        Lazily re-initializes the client if it's found to be empty.
        """
        if not self._client:
            logger.warning("Client was empty, attempting re-initialization.")
            self._initialize_clients()
        assert self._client is not None, "Client should be initialized"
        return self._client
    
    @property
    def account_url(self) -> str:
        """
        Returns the base URL of the storage account.
        """
        return self._account_url
    
    @property
    def container_name(self) -> str:
        """
        Returns the name of the container, which is the same as the resource name.
        """
        return self._resource_name