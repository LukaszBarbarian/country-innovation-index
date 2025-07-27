# src/common/storage/bronze_storage_manager.py

from __future__ import annotations
import json
from datetime import datetime
from typing import Any, Union, Dict, Literal
import logging 
from src.common.storage_account.blob_storage_manager import BlobStorageManager
from src.functions.common.processors.processed_result import ProcessedResult

logger = logging.getLogger(__name__)

class BronzeStorageManager(BlobStorageManager):
    """
    Menedżer do interakcji z kontenerem 'bronze' w Azure Blob Storage.
    Dziedziczy generyczne operacje na blob-ach z BlobStorageManager
    i może dodawać specyficzne metody dla warstwy Bronze.
    """
    DEFAULT_CONTAINER_NAME = "bronze"

    def __init__(self):
        super().__init__(self.DEFAULT_CONTAINER_NAME) 
        logger.info(f"BronzeStorageManager initialized for container: {self.resource_name}") 
   

    def upload_processed_result(self, result: ProcessedResult, blob_path: str) -> int:
        try:
            content = result.serialize()
            return self.upload_blob(content, blob_name=blob_path)
        except Exception as e:
            logger.error(f"Nie udało się wgrać ProcessedResult do '{blob_path}': {e}")
            raise


        return 0
