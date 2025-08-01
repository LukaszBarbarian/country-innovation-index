# src/common/storage_account/bronze_storage_manager.py

import logging
from src.common.models.file_info import FileInfo 
from src.common.storage_account.blob_storage_manager import BlobStorageManager

logger = logging.getLogger(__name__)

class BronzeStorageManager(BlobStorageManager):
    DEFAULT_CONTAINER_NAME = "bronze" 

    def __init__(self):
        # Wywołujemy konstruktor klasy bazowej, przekazując nazwę kontenera
        # i nazwę zmiennej środowiskowej dla konta storage.
        super().__init__(
            container_name=self.DEFAULT_CONTAINER_NAME,
            storage_account_name_setting_name="DATA_LAKE_STORAGE_ACCOUNT_NAME"
        )
        # self.service_client (BlobServiceClient) i self.client (ContainerClient)
        # są już dostępne z klasy bazowej, więc linia self.blob_service_client = self.service_client jest zbędna.
        logger.info(f"BronzeStorageManager initialized for container: {self.DEFAULT_CONTAINER_NAME}")

    async def upload_file(self, file_content_bytes: bytes, file_info: FileInfo) -> int:
        """
        Przyjmuje gotowe bajty pliku i FileInfo, i zapisuje je do bloba.
        Używa metody upload_blob z klasy bazowej.
        Zwraca rozmiar zapisanego pliku w bajtach.
        """
        # Sprawdzamy, czy kontener z FileInfo zgadza się z domyślnym kontenerem managera.
        # Możesz to pominąć, jeśli zawsze używasz DEFAULT_CONTAINER_NAME.
        if file_info.container_name != self.DEFAULT_CONTAINER_NAME:
            logger.warning(f"FileInfo specifies container '{file_info.container_name}', but manager is for '{self.DEFAULT_CONTAINER_NAME}'. Using manager's default.")
        
        try:
            folder_path = "/".join(file_info.full_path_in_container.split("/")[:-1]) + "/"
            if await self.blob_with_same_payload_hash_exists(folder_path, file_info.payload_hash):
                logger.info(f"File with payload_hash {file_info.payload_hash} already exists in {folder_path}. Skipping upload.")
                return 0

            await self.upload_blob( # Wywołanie metody z klasy bazowej
                data_content=file_content_bytes, 
                blob_name=file_info.full_path_in_container, # To jest już "ścieżka/do/pliku.json"
                overwrite=True
                #tags=file_info.blob_tags if file_info.blob_tags else None # Przekazujemy tagi
            )
            
            logger.info(f"Successfully uploaded file to {file_info.full_path_in_container}. Size: {len(file_content_bytes)} bytes. Tags: {file_info.blob_tags}")
            return len(file_content_bytes) # Zwracamy rzeczywisty rozmiar
        except Exception as e:
            logger.error(f"Failed to upload file to {file_info.full_path_in_container}: {e}", exc_info=True)
            raise

