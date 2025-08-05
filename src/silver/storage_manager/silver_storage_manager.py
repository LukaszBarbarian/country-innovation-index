# src/common/storage/silver_storage_manager.py

import logging
from typing import Dict, Any, Union, Literal
from src.common.storage_account.blob_storage_manager import BlobStorageManager

logger = logging.getLogger(__name__)

class SilverStorageManager(BlobStorageManager):
    """
    Menedżer do interakcji z kontenerem 'silver' w Azure Blob Storage.
    Dziedziczy generyczne operacje na blob-ach z BlobStorageManager
    i może dodawać specyficzne metody dla warstwy Silver (danych przetworzonych).
    """
    DEFAULT_CONTAINER_NAME = "silver" # Ustaw domyślną nazwę kontenera

    def __init__(self):
        super().__init__(container_name=self.DEFAULT_CONTAINER_NAME, storage_account_name_setting_name="DATA_LAKE_STORAGE_ACCOUNT_NAME")
        logger.info(f"SilverStorageManager initialized for container: {self.resource_name}")

    # Przykład metody specyficznej dla Silver (jeśli potrzebna)
    def store_processed_data(self, dataset_name: str, processed_data: Dict[str, Any]) -> str:
        """
        Przechowuje przetworzone dane do warstwy Silver.
        Generuje ścieżkę bloba w formacie: {dataset_name}/data.json
        """
        blob_name = f"{dataset_name}/data.json" # Przykładowa ścieżka
        logger.info(f"Storing processed data for dataset '{dataset_name}' to Silver at path: {blob_name}")
        return self.upload_blob(processed_data, blob_name, overwrite=True)

    # Możesz nadpisać upload/download, jeśli potrzebujesz specjalnej logiki dla Silver
    # np. walidacji schematu przed zapisem.