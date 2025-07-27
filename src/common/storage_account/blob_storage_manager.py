# src/common/storage/blob_storage_manager.py

import logging
from typing import Any, Union, Dict, Literal, Optional
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError
import json
from src.common.storage_account.manager_base import AzureClientManagerBase
from src.common.storage_metadata_file_builder.base_storage_path_builder import BaseStoragePathBuilder
from src.common.storage_metadata_file_builder.default_storage_metadata_file_builder import DefaultStoragePathBuilder

logger = logging.getLogger(__name__)

class BlobStorageManager(AzureClientManagerBase[BlobServiceClient, ContainerClient]):
    """
    Menedżer do interakcji z Azure Blob Storage dla określonego kontenera.
    Implementuje metody abstrakcyjne z AzureClientManagerBase dla Blob Storage.
    Zawiera generyczne metody wgrywania i pobierania blobów.
    """
    def __init__(self, 
                 container_name: str, 
                 connection_string_setting_name: str = "AzureWebJobsStorage"):
        """
        Inicjalizuje menedżera Blob Storage.

        Args:
            container_name (str): Nazwa kontenera Blob Storage.
            connection_string_setting_name (str): Nazwa ustawienia connection string w ConfigManagerze.
                                                  Domyślnie "AzureWebJobsStorage".
        """
        super().__init__(
            resource_name=container_name,
            connection_string_setting_name=connection_string_setting_name,
            base_url_suffix=".blob.core.windows.net"
        )

        logger.info(f"BlobStorageManager initialized for container: {self.resource_name}")

    def _create_service_client_from_connection_string(self, connection_string: str) -> BlobServiceClient:
        return BlobServiceClient.from_connection_string(connection_string)

    def _create_service_client_from_identity(self, account_url: str, credential) -> BlobServiceClient:
        return BlobServiceClient(account_url=account_url, credential=credential)

    def _get_resource_client(self, service_client: BlobServiceClient, container_name: str) -> ContainerClient:
        return service_client.get_container_client(container_name)

    # --- Generyczne metody dla Blobów, które mogą być używane przez Bronze/Silver/Gold ---

    def upload_blob(self, 
                    data_content: Union[Dict[str, Any], str, bytes], 
                    blob_name: str,     
                    overwrite: bool = True
                   ) -> int:
        """
        Wgrywa zawartość danych do określonej ścieżki bloba w kontenerze.
        
        Args:
            data_content (Union[Dict[str, Any], str, bytes]): Dane do wgrania.
                                                              Jeśli `dict`, zostanie zserializowany do JSON.
                                                              Jeśli `str`, zostanie zakodowany do UTF-8.
            blob_name (str): Pełna ścieżka do bloba w kontenerze (np. "api_name/rok/miesiac/dzien/plik.json").
            overwrite (bool): Czy nadpisać istniejący blob.

        Returns:
            str: Pełna ścieżka do bloba (nazwa_kontenera/nazwa_bloba).
        
        Raises:
            TypeError: Jeśli typ data_content jest nieobsługiwany.
            Exception: Jeśli wystąpi błąd podczas operacji na storage.
        """
        
        # Konwersja danych do bajtów, jeśli to konieczne
        if isinstance(data_content, dict): 
            content_to_upload = json.dumps(data_content, indent=2).encode('utf-8')
        elif isinstance(data_content, str):
            content_to_upload = data_content.encode('utf-8')
        elif isinstance(data_content, bytes):
            content_to_upload = data_content
        else:
            logger.error(f"Nieobsługiwany typ danych do wgrania: {type(data_content)}. Musi być dict, str lub bytes.")
            raise TypeError(f"Nieobsługiwany typ danych do wgrania: {type(data_content)}. Musi być dict, str lub bytes.")

        try:
            # self.client to teraz ContainerClient
            blob_client = self.client.get_blob_client(blob_name)
            blob_client.upload_blob(content_to_upload, overwrite=overwrite)
            
            logger.info(f"Wgrano dane do {self.resource_name}/{blob_name}")
            return f"{self.resource_name}/{blob_name}"
        except Exception as e:
            logger.error(f"Błąd podczas wgrywania bloba '{blob_name}': {e}")
            raise 

    def download_blob(self, blob_path: str, decode_as: Union[None, Literal['text'], Literal['json']] = None) -> Any:
        """
        Pobiera dane z bloba.
        
        Args:
            blob_path (str): Pełna ścieżka do bloba w kontenerze.
            decode_as (Union[None, Literal['text'], Literal['json']]): Sposób dekodowania danych.
                - None: zwraca surowe bajty (bytes).
                - 'text': zwraca string (dekodowany jako UTF-8).
                - 'json': próbuje sparsować jako JSON.
        
        Returns:
            Any: Dekodowane dane (dict, str) lub surowe bajty (bytes).
        
        Raises:
            ValueError: Jeśli nie uda się zdekodować JSON.
            ResourceNotFoundError: Jeśli blob nie istnieje.
            Exception: Jeśli wystąpi inny błąd podczas operacji na storage.
        """
        try:
            # self.client to teraz ContainerClient
            blob_client = self.client.get_blob_client(blob_path)
            download_stream = blob_client.download_blob()
            data_bytes = download_stream.readall()
            
            if decode_as == 'json':
                return json.loads(data_bytes)
            elif decode_as == 'text':
                return data_bytes.decode('utf-8')
            return data_bytes 
        except ResourceNotFoundError:
            logger.error(f"Blob '{blob_path}' nie znaleziono w kontenerze '{self.resource_name}'.")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Błąd dekodowania JSON dla bloba '{blob_path}': {e}")
            raise ValueError(f"Nieprawidłowy format JSON dla bloba '{blob_path}'.")
        except Exception as e:
            logger.error(f"Błąd podczas pobierania bloba '{blob_path}': {e}")
            raise

    def list_blobs(self, name_starts_with: Optional[str] = None) -> list[str]:
        """
        Listuje nazwy blobów w kontenerze.
        
        Args:
            name_starts_with (Optional[str]): Prefiks do filtrowania nazw blobów.
        
        Returns:
            list[str]: Lista nazw blobów.
        """
        try:
            blob_list = []
            for blob in self.client.list_blobs(name_starts_with=name_starts_with):
                blob_list.append(blob.name)
            logger.info(f"Listed {len(blob_list)} blobs in container '{self.resource_name}' with prefix '{name_starts_with or ''}'.")
            return blob_list
        except Exception as e:
            logger.error(f"Error listing blobs in container '{self.resource_name}': {e}")
            raise

    def delete_blob(self, blob_name: str):
        """
        Usuwa blob z kontenera.
        """
        try:
            self.client.delete_blob(blob_name)
            logger.info(f"Blob '{blob_name}' deleted from container '{self.resource_name}'.")
        except ResourceNotFoundError:
            logger.warning(f"Attempted to delete non-existent blob '{blob_name}' in container '{self.resource_name}'.")
        except Exception as e:
            logger.error(f"Error deleting blob '{blob_name}': {e}")
            raise