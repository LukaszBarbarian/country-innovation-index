# src/common/storage/gold_storage_manager.py

import logging
from typing import Dict, Any, Union, Literal
from datetime import datetime
from src.common.storage_account.blob_storage_manager import BlobStorageManager

logger = logging.getLogger(__name__)

class GoldStorageManager(BlobStorageManager):
    """
    Menedżer do interakcji z kontenerem 'gold' w Azure Blob Storage.
    Dziedziczy generyczne operacje na blob-ach z BlobStorageManager
    i może dodawać specyficzne metody dla warstwy Gold (danych analitycznych/raportowych).
    """
    DEFAULT_CONTAINER_NAME = "gold" # Ustaw domyślną nazwę kontenera

    def __init__(self):
        super().__init__(self.DEFAULT_CONTAINER_NAME)
        logger.info(f"GoldStorageManager initialized for container: {self.resource_name}")

    # Przykład metody specyficznej dla Gold (jeśli potrzebna)
    def save_report_data(self, report_name: str, report_data: Dict[str, Any]) -> str:
        """
        Zapisuje dane raportowe/agregaty do warstwy Gold.
        """
        blob_name = f"reports/{report_name}/{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        logger.info(f"Saving report '{report_name}' data to Gold at path: {blob_name}")
        return self.upload_blob(report_data, blob_name, overwrite=True)