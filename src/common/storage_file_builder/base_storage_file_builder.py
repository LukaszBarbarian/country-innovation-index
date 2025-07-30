# src/common/storage_metadata_file_builder/base_storage_metadata_file_builder.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import uuid

from src.common.contexts.bronze_context import BronzeContext
from src.common.models.processed_result import ProcessedResult


class BaseStorageFileBuilder(ABC):
    @abstractmethod
    def build_file_output(self, 
                          processed_records_results: List[ProcessedResult], 
                          context: BronzeContext, 
                          storage_account_name: str, 
                          container_name: str) -> Dict[str, Any]:
        """
        Przyjmuje listę obiektów ProcessedResult i kontekst ingestii.
        Tworzy finalny string JSON (lub bajty dla innych formatów),
        generuje nazwę pliku/ścieżkę i tworzy obiekt file_info wraz z tagami bloba.
        Zwraca słownik zawierający 'file_content_bytes' oraz 'file_info'.
        """
        pass
    
    def _generate_blob_path_and_name(self, context: BronzeContext, file_extension: str):
        """
        Generuje pełną ścieżkę do bloba i nazwę pliku, uwzględniając rozszerzenie.
        Zwraca krotkę (full_path_in_container, file_name).
        """
        date_path = context.ingestion_time_utc.strftime("%Y/%m/%d") # Np. 2025/07/29
        time_component = context.ingestion_time_utc.strftime("%H%M%S") # Np. 094500

        unique_id = str(uuid.uuid4())
        # Nazwa pliku z correlation_id i rozszerzeniem
        file_name = f"{context.correlation_id}_{time_component}_{unique_id}.{file_extension.lstrip('.')}"
 
        full_path_in_container = f"{context.dataset_name}/{date_path}/{file_name}"
        return full_path_in_container, file_name