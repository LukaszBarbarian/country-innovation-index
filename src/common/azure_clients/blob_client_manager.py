# src/common/storage/blob_storage_manager.py

import logging
from typing import Any, Union, Dict, Literal, Optional
# Zmieniamy na asynchroniczne klienty
from azure.storage.blob.aio import BlobServiceClient, ContainerClient 
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError 
import json
from src.common.models.file_info import FileInfo
from src.common.azure_clients.base_azure_client_manager import AzureClientManagerBase

logger = logging.getLogger(__name__)

class BlobClientManager(AzureClientManagerBase[BlobServiceClient, ContainerClient]):
    
    def __init__(self, 
                 container_name: str, 
                 storage_account_name_setting_name: str = "DATA_LAKE_STORAGE_ACCOUNT_NAME"):
        
        super().__init__(
            resource_name=container_name,
            storage_account_name_setting_name=storage_account_name_setting_name,
            base_url_suffix=".blob.core.windows.net"
        )

    def _create_service_client_from_identity(self, account_url: str, credential) -> BlobServiceClient:
        return BlobServiceClient(account_url=account_url, credential=credential)

    def _get_resource_client(self, service_client: BlobServiceClient, resource_name: str) -> ContainerClient:
        return service_client.get_container_client(resource_name)

    async def upload_blob(self, 
                          file_content_bytes: Union[Dict[str, Any], str, bytes], 
                          file_info: FileInfo,
                          overwrite: bool = True,
                          tags: Optional[Dict[str, str]] = None
                         ) -> int:
        
        # Konwersja danych do bajtów, jeśli to konieczne
        if isinstance(file_content_bytes, dict): 
            content_to_upload = json.dumps(file_content_bytes, indent=2).encode('utf-8')
        elif isinstance(file_content_bytes, str):
            content_to_upload = file_content_bytes.encode('utf-8')
        elif isinstance(file_content_bytes, bytes):
            content_to_upload = file_content_bytes
        else:
            logger.error(f"Unsupported data type for upload: {type(file_content_bytes)}. Must be dict, str, or bytes.")
            raise TypeError(f"Unsupported data type for upload: {type(file_content_bytes)}. Must be dict, str, or bytes.")

        # --- ZMIANA TUTAJ: Ręczna obsługa tworzenia kontenera i istniejącego zasobu ---
        try:
            await self.client.create_container()
        except ResourceExistsError:
            logger.debug(f"Container '{self.resource_name}' already exists. Skipping creation.")
        except Exception as container_creation_error:
            logger.error(f"Unexpected error when creating container '{self.resource_name}': {container_creation_error}", exc_info=True)
            raise # Rzuć ponownie inne, nieoczekiwane błędy tworzenia kontenera
        # --- KONIEC ZMIANY ---

        try:
            folder_path = "/".join(file_info.full_path_in_container.split("/")[:-1]) + "/"
            if await self.blob_with_same_payload_hash_exists(folder_path, file_info.payload_hash):
                logger.info(f"File with payload_hash {file_info.payload_hash} already exists in {folder_path}. Skipping upload.")
                return 0

            blob_name = file_info.full_path_in_container
            blob_client = self.client.get_blob_client(blob_name)
            
            # --- ZMIANA TUTAJ: Usuwamy parametr 'tags' aby uniknąć błędu "FeatureNotYetSupportedForHierarchicalNamespaceAccounts" ---
            # Jeśli w przyszłości BARDZO potrzebujesz tagów, będziesz musiał to przemyśleć ponownie.
            # Ale na razie, dla "zwykłego wrzucenia pliku", usunięcie tagów jest kluczowe.
            await blob_client.upload_blob(content_to_upload, overwrite=overwrite) 
            # --- KONIEC ZMIANY ---
            
            logger.info(f"Data uploaded successfully to {self.resource_name}/{blob_name}")
            
            return len(file_content_bytes)
        except Exception as e:
            logger.error(f"Failed to upload file to {file_info.full_path_in_container}: {e}", exc_info=True)
            raise

    


    async def download_blob(self, blob_path: str, decode_as: Union[None, Literal['text'], Literal['json']] = None) -> Any:
        """
        Pobiera dane z bloba.
        """
        try:
            blob_client = self.client.get_blob_client(blob_path)
            download_stream = await blob_client.download_blob() # Zmiana na await
            data_bytes = await download_stream.readall() # Zmiana na await
            
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
            logger.error(f"Błąd podczas pobierania bloba '{blob_path}': {e}", exc_info=True)
            raise

    async def list_blobs(self, name_starts_with: Optional[str] = None) -> list[str]:
        """
        Listuje nazwy blobów w kontenerze.
        """
        try:
            blob_list = []
            async for blob in self.client.list_blobs(name_starts_with=name_starts_with): # Zmiana na async for
                blob_list.append(blob.name)
            logger.info(f"Listed {len(blob_list)} blobs in container '{self.resource_name}' with prefix '{name_starts_with or ''}'.")
            return blob_list
        except Exception as e:
            logger.error(f"Error listing blobs in container '{self.resource_name}': {e}", exc_info=True)
            raise

    async def delete_blob(self, blob_name: str):
        """
        Usuwa blob z kontenera.
        """
        try:
            await self.client.delete_blob(blob_name) # Zmiana na await
            logger.info(f"Blob '{blob_name}' deleted from container '{self.resource_name}'.")
        except ResourceNotFoundError:
            logger.warning(f"Attempted to delete non-existent blob '{blob_name}' in container '{self.resource_name}'.")
        except Exception as e:
            logger.error(f"Error deleting blob '{blob_name}': {e}", exc_info=True)
            raise

    # Dodajemy metody asynchroniczne do context managera
    async def __aenter__(self):
        # self.client (ContainerClient) jest już zainicjalizowany w __init__ bazowej klasy
        # Tutaj można by dodać await self.client.create_container(fail_on_exist=False)
        # jeśli chcesz mieć pewność, że kontener istnieje zanim zaczniesz cokolwiek robić.
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.close() # Zamknij ContainerClient
        # Dodatkowo, jeśli chcesz zamknąć BlobServiceClient, musisz go przechowywać w klasie bazowej
        if hasattr(self, '_service_client') and self._service_client:
            await self._service_client.close()



    async def blob_with_same_payload_hash_exists(self, folder_path: str, payload_hash: str) -> bool:
        """
        Sprawdza, czy w folderze (prefix) jest blob z ciągiem 'payload_hash' w nazwie.
        
        Args:
            folder_path: Ścieżka do folderu, np. "folder1/folder2/".
            payload_hash: Hash, który ma być wyszukany w nazwie bloba.
            
        Returns:
            True, jeśli blob o pasującej nazwie istnieje, False w przeciwnym wypadku.
        """
        try:
            # Upewnij się, że name_starts_with ma poprawny format
            prefix = folder_path if folder_path.endswith('/') else folder_path + '/'
            
            # Pobieranie blobów z prefixem folderu
            blobs = self.client.list_blobs(name_starts_with=prefix)
            
            async for blob in blobs:
                # Warunek sprawdzający, czy podany hash znajduje się w nazwie bloba
                if payload_hash in blob.name:
                    logger.info(f"Znaleziono istniejący blob '{blob.name}' zawierający hash: {payload_hash}")
                    return True
            
            # Jeśli pętla się zakończyła i nic nie znaleziono
            return False
            
        except Exception as e:
            logger.error(f"Wystąpił błąd podczas sprawdzania blobów pod kątem hasha '{payload_hash}': {e}", exc_info=True)
            raise