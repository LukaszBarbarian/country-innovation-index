# src/common/storage/bronze_storage_manager.py

from __future__ import annotations
import json
from datetime import datetime
from typing import Any, Union, Dict, Literal
import logging 

from src.common.storage_account.blob_storage_manager import BlobStorageManager

logger = logging.getLogger(__name__)

class BronzeStorageManager(BlobStorageManager):
    """
    Menedżer do interakcji z kontenerem 'bronze' w Azure Blob Storage.
    Dziedziczy generyczne operacje na blob-ach z BlobStorageManager
    i może dodawać specyficzne metody dla warstwy Bronze.
    """
    DEFAULT_CONTAINER_NAME = "bronze" # Ustaw domyślną nazwę kontenera

    def __init__(self):
        # Wywołaj konstruktor klasy bazowej (BlobStorageManager) z nazwą kontenera
        super().__init__(self.DEFAULT_CONTAINER_NAME) 
        logger.info(f"BronzeStorageManager initialized for container: {self.resource_name}") 

    # --- Twoje istniejące metody upload_blob i download_blob są już w BlobStorageManager ---
    # Jeśli chcesz je nadpisać z dodatkową logiką specyficzną dla Bronze, możesz to zrobić tutaj.
    # W przeciwnym razie, będą one dziedziczone bezpośrednio.

    # Przykład metody specyficznej dla Bronze (jeśli potrzebna)
    def ingest_data_from_api(self, api_name: str, data: Dict[str, Any]) -> str:
        """
        Metoda do ingestowania danych z API do warstwy Bronze.
        Automatycznie generuje ścieżkę bloba w formacie:
        {api_name}/rok/miesiac/dzien/hhmmss_unikalneid.json
        """
        now = datetime.now()
        blob_name = f"{api_name}/{now.year}/{now.month:02d}/{now.day:02d}/{now.strftime('%H%M%S')}_{now.microsecond}.json"
        
        logger.info(f"Ingesting data for API '{api_name}' to Bronze at path: {blob_name}")
        return self.upload_blob(data, blob_name) # Używamy metody upload_blob z klasy bazowej

    # Możesz dodać inne metody specyficzne dla Bronze, np. do zarządzania surowymi plikami logów itp.