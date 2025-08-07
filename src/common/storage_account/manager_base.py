# src/common/storage/base_azure_client_manager.py

from abc import ABC, abstractmethod
import logging
from typing import Optional, TypeVar, Generic

from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError
from src.common.config.config_manager import ConfigManager

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

    def __init__(self, resource_name: str, storage_account_name_setting_name: str, base_url_suffix: str):
        if not resource_name:
            raise ValueError("Nazwa zasobu nie może być pusta.")
        if not storage_account_name_setting_name:
            raise ValueError("Nazwa ustawienia connection string nie może być pusta.")
        if not base_url_suffix:
            raise ValueError("Sufiks URL nie może być pusty.")

        self._resource_name: str = resource_name
        self._storage_account_name_setting_name: str = storage_account_name_setting_name
        self._base_url_suffix: str = base_url_suffix

        self._service_client: Optional[TServiceClient] = None
        self._client: Optional[TClient] = None

        self._initialize_clients()
        logger.info(f"AzureClientManagerBase initialized for resource '{self.resource_name}'.")


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
        storage_account_name = self._config.get_setting(self._storage_account_name_setting_name)
        
        
        if not self._service_client and storage_account_name: # Jeśli connection string zawiódł lub go nie było
            logger.info(f"Próba połączenia z Azure za pomocą Azure Identity dla konta: {storage_account_name}.")
            try:
                credential = DefaultAzureCredential()
                account_url = f"https://{storage_account_name}{self._base_url_suffix}"
                self._account_url = account_url


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
        if not self._service_client:
            logger.warning("ServiceClient był pusty, próba ponownej inicjalizacji.")
            self._initialize_clients()
        assert self._service_client is not None, "ServiceClient powinien być zainicjalizowany"
        return self._service_client


    @property
    def client(self) -> TClient:
        if not self._client:
            logger.warning("Client był pusty, próba ponownej inicjalizacji.")
            self._initialize_clients()
        assert self._client is not None, "Client powinien być zainicjalizowany"
        return self._client
    


    @property
    def account_url(self) -> str:
        return self._account_url
    
    @property
    def container_name(self) -> str:
        return self._resource_name