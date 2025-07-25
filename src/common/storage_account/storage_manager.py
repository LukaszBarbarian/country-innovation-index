# src/common/storage/base_storage_manager.py

from abc import ABC
import logging
from typing import Optional

# Importuj potrzebne klasy z Azure SDK
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential # Do autoryzacji Managed Identity/Service Principal

# Import ConfigManagera
from src.functions.common.config.config_manager import ConfigManager

logger = logging.getLogger(__name__)

class StorageManagerBase(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich menedżerów przechowywania (np. Bronze, Silver, Gold).
    Odpowiada za inicjalizację połączenia z Azure Blob Storage dla określonego kontenera.
    Obsługuje autoryzację za pomocą connection string lub Azure Identity (Managed Identity/Service Principal).
    """

    _config: ConfigManager = ConfigManager()

    def __init__(self, container_name: str):
        """
        Inicjalizuje bazowego menedżera przechowywania.
        
        Args:
            container_name (str): Nazwa kontenera Azure Blob Storage, z którym menedżer będzie współpracował.
        """
        if not container_name:
            raise ValueError("Nazwa kontenera nie może być pusta.")
        
        self._container_name: str = container_name
        self._blob_service_client: Optional[BlobServiceClient] = None
        self._container_client: Optional[ContainerClient] = None
        
        self._initialize_clients()
        logger.info(f"StorageManagerBase initialized for container '{self.container_name}'.")

    def _initialize_clients(self):
        """
        Inicjalizuje BlobServiceClient i ContainerClient.
        Preferuje uwierzytelnianie za pomocą connection string,
        a w razie braku lub błędu - próbuje Azure Identity (Managed Identity/Service Principal).
        """
        connection_string = self._config.get_setting("AzureWebJobsStorage") # Standardowa nazwa dla Functions
        storage_account_name = self._container_name

        # 1. Próba użycia connection string (dobry do dev lokalnego)
        if connection_string:
            logger.info("Próba połączenia z Azure Blob Storage za pomocą connection string.")
            try:
                self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
                # Szybkie sprawdzenie, czy klient działa
                self._blob_service_client.get_user_delegation_key(start_time="2023-01-01T00:00:00Z", expiry_time="2023-01-01T01:00:00Z") # Metoda, która wymaga połączenia, ale nie uprawnień do kontenera
                logger.info("Połączenie z Azure Blob Storage za pomocą connection string udane.")
                # Jeśli udało się z connection string, nie próbuj innych metod
                return 
            except Exception as e:
                logger.warning(f"Connection string dostępny, ale błąd podczas inicjalizacji BlobServiceClient: {e}. Próba użycia Azure Identity.")
                self._blob_service_client = None # Upewnij się, że jest None, aby spróbować kolejnej metody
        
        # 2. Próba użycia Azure Identity (zalecane w Azure)
        if storage_account_name:
            logger.info(f"Próba połączenia z Azure Blob Storage za pomocą Azure Identity dla konta: {storage_account_name}.")
            try:
                # DefaultAzureCredential spróbuje wielu metod uwierzytelniania:
                # zmienne środowiskowe, tożsamość zarządzana (Managed Identity), Visual Studio Code, Azure CLI itd.
                credential = DefaultAzureCredential()
                
                account_url = f"https://{storage_account_name}.blob.core.windows.net"
                self._blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
                
                # Szybkie sprawdzenie, czy klient działa (np. próba listowania kontenerów)
                # Należy uważać, aby nie wywoływać metody, która wymaga szerokich uprawnień, których może nie mieć Managed Identity.
                # Proste listowanie kontenerów, nawet jeśli lista będzie pusta, sprawdzi uwierzytelnienie.
                # Poniższa linia może wymagać uprawnień do 'Storage Blob Data Reader' na poziomie konta storage.
                # Jeśli to za dużo, można ją pominąć i polegać na błędach podczas faktycznych operacji na kontenerze.
                # list(self._blob_service_client.list_containers(name_starts_with=self.container_name, results_per_page=1)) 
                
                logger.info(f"Połączenie z Azure Blob Storage za pomocą Azure Identity dla konta {storage_account_name} udane.")
                
            except Exception as e:
                logger.error(f"Błąd podczas tworzenia BlobServiceClient z Azure Identity dla konta {storage_account_name}: {e}")
                raise RuntimeError(f"Nie udało się zainicjować połączenia z Azure Blob Storage: {e}")

        if not self._blob_service_client:
            raise RuntimeError("Nie można zainicjować BlobServiceClient. Brak prawidłowego connection stringu lub nazwy konta storage/poświadczeń.")

        # Pobieramy klienta dla konkretnego kontenera
        self._container_client = self._blob_service_client.get_container_client(self.container_name)


    @property
    def container_name(self) -> str:
        """Zwraca nazwę kontenera, z którym menedżer współpracuje."""
        return self._container_name

    @property
    def blob_service_client(self) -> BlobServiceClient:
        """Zwraca instancję BlobServiceClient."""
        if not self._blob_service_client:
            # Ta linia powinna być rzadko wywoływana, bo klient jest inicjalizowany w __init__
            logger.warning("BlobServiceClient był pusty, próba ponownej inicjalizacji.")
            self._initialize_clients() 
        return self._blob_service_client

    @property
    def container_client(self) -> ContainerClient:
        """Zwraca instancję ContainerClient dla przypisanego kontenera."""
        if not self._container_client:
            # Ta linia również powinna być rzadko wywoływana
            logger.warning("ContainerClient był pusty, próba ponownej inicjalizacji.")
            self._initialize_clients() 
        return self._container_client
