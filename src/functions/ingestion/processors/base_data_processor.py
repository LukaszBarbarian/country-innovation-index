# src/ingestion/processors/base_data_processor.py
from abc import ABC, abstractmethod
from typing import Any
from src.functions.common.storage_account.bronze_storage_manager import BronzeStorageManager
from src.functions.common.models.ingestion_context import IngestionContext

class BaseDataProcessor(ABC):
    def __init__(self, storage_manager: BronzeStorageManager):
        self.storage_manager = storage_manager

    @abstractmethod
    def process_and_save(self, context: IngestionContext) -> str:
        """Przetwarza surowe dane z kontekstu i zapisuje je do storage. Zwraca ścieżkę do bloba."""
        pass