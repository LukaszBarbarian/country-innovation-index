# src/common/storage/base_azure_client_manager.py

from abc import ABC, abstractmethod
import logging
from typing import Optional, TypeVar, Generic

from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError


from src.functions.common.config.config_manager import ConfigManager

logger = logging.getLogger(__name__)

# Typy dla generycznych klientów
TServiceClient = TypeVar('TServiceClient')
TClient = TypeVar('TClient')

class AzureClientManagerBase(ABC, Generic[TServiceClient, TClient]):
    """
    Abstrakcyjna klasa bazowa dla wszystkich menedżerów klientów Azure SDK (np. Blob, Queue).
    Odpowiada za inicjalizację połączenia z określoną usługą Azure.
    Obsługuje autoryzację za pomocą connection string lub Azure Identity (Managed Identity/Service Principal).
    """

    _config: ConfigManager = ConfigManager()

    def __init__(self, resource_name: str, connection_string_setting_name: str, base_url_suffix: str):
        """
        Inicjalizuje bazowego menedżera klientów Azure SDK.

        Args:
            resource_name (str): Nazwa konkretnego zasobu (np. nazwa kontenera, nazwa kolejki).
            connection_string_setting_name (str): Nazwa ustawienia w ConfigManagerze,
                                                  gdzie znajduje się connection string dla tej usługi.
            base_url_suffix (str): Sufiks URL dla danej usługi (np. ".blob.core.windows.net", ".queue.core.windows.net").
        """
        if not resource_name:
            raise ValueError("Nazwa zasobu nie może być pusta.")
        if not connection_string_setting_name:
            raise ValueError("Nazwa ustawienia connection string nie może być pusta.")
        if not base_url_suffix:
            raise ValueError("Sufiks URL nie może być pusty.")

        self._resource_name: str = resource_name
        self._connection_string_setting_name: str = connection_string_setting_name
        self._base_url_suffix: str = base_url_suffix

        self._service_client: Optional[TServiceClient] = None
        self._client: Optional[TClient] = None

        self._initialize_clients()
        logger.info(f"AzureClientManagerBase initialized for resource '{self.resource_name}'.")

    @abstractmethod
    def _create_service_client_from_connection_string(self, connection_string: str) -> TServiceClient:
        """Abstrakcyjna metoda do tworzenia ServiceClient z connection string."""
        pass

    @abstractmethod
    def _create_service_client_from_identity(self, account_url: str, credential) -> TServiceClient:
        """Abstrakcyjna metoda do tworzenia ServiceClient z Azure Identity."""
        pass

    @abstractmethod
    def _get_resource_client(self, service_client: TServiceClient, resource_name: str) -> TClient:
        """Abstrakcyjna metoda do pobierania klienta dla konkretnego zasobu (kontenera/kolejki)."""
        pass

    def _initialize_clients(self):
        """
        Inicjalizuje ServiceClient i klienta zasobu.
        Preferuje uwierzytelnianie za pomocą connection string,
        a w razie braku lub błędu - próbuje Azure Identity (Managed Identity/Service Principal).
        """
        connection_string = self._config.get_setting(self._connection_string_setting_name)
        
        # Próba parsowania nazwy konta z connection stringu (dla Azure Identity fallback)
        storage_account_name = None
        if connection_string:
            for part in connection_string.split(';'):
                if part.startswith('AccountName='):
                    storage_account_name = part.split('=', 1)[1]
                    break
        
        # 1. Próba użycia connection string
        if connection_string:
            logger.info(f"Próba połączenia z Azure za pomocą connection string z '{self._connection_string_setting_name}'.")
            try:
                self._service_client = self._create_service_client_from_connection_string(connection_string)
                logger.info("Połączenie za pomocą connection string udane.")
            except Exception as e:
                logger.warning(f"Connection string dostępny, ale błąd podczas inicjalizacji ServiceClient: {e}. Próba użycia Azure Identity.")
                self._service_client = None
        
        # 2. Próba użycia Azure Identity (zalecane w Azure)
        if not self._service_client and storage_account_name: # Jeśli connection string zawiódł lub go nie było
            logger.info(f"Próba połączenia z Azure za pomocą Azure Identity dla konta: {storage_account_name}.")
            try:
                credential = DefaultAzureCredential()
                account_url = f"https://{storage_account_name}{self._base_url_suffix}"
                self._service_client = self._create_service_client_from_identity(account_url, credential)
                logger.info(f"Połączenie za pomocą Azure Identity dla konta {storage_account_name} udane.")
            except Exception as e:
                logger.error(f"Błąd podczas tworzenia ServiceClient z Azure Identity dla konta {storage_account_name}: {e}")
                raise RuntimeError(f"Nie udało się zainicjować połączenia z Azure: {e}")

        if not self._service_client:
            raise RuntimeError("Nie można zainicjować ServiceClient. Brak prawidłowego connection stringu lub nazwy konta storage/poświadczeń.")

        # Pobieramy klienta dla konkretnego zasobu (kontenera/kolejki)
        self._client = self._get_resource_client(self._service_client, self.resource_name)

    @property
    def resource_name(self) -> str:
        """Zwraca nazwę zasobu (kontenera/kolejki), z którym menedżer współpracuje."""
        return self._resource_name

    @property
    def service_client(self) -> TServiceClient:
        """Zwraca instancję ServiceClient."""
        if not self._service_client:
            logger.warning("ServiceClient był pusty, próba ponownej inicjalizacji.")
            self._initialize_clients()
        return self._service_client

    @property
    def client(self) -> TClient:
        """Zwraca instancję klienta dla przypisanego zasobu (kontenera/kolejki)."""
        if not self._client:
            logger.warning("Client był pusty, próba ponownej inicjalizacji.")
            self._initialize_clients()
        return self._client