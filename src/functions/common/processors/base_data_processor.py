from abc import ABC, abstractmethod
from typing import Any
from src.common.storage_account.bronze_storage_manager import BronzeStorageManager
from src.functions.common.models.ingestion_context import IngestionContext
from src.functions.common.processors.processed_result import ProcessedResult

class BaseDataProcessor(ABC):
    def __init__(self, storage_manager: BronzeStorageManager):
        self.storage_manager = storage_manager

    @abstractmethod
    def process(self, context: IngestionContext) -> ProcessedResult:
        pass

    def save(self, context: IngestionContext, processed_data: Any) -> str:
        return "" #self.storage_manager.save(context, processed_data)
