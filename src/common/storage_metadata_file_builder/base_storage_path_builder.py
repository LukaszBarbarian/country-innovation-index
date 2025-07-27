# src/common/file_metadata/base_file_metadata_builder.py (nowy plik/ścieżka)
from abc import ABC, abstractmethod
from typing import Dict, Any
from src.common.models.file_info import FileInfo
from src.common.enums.domain_source import DomainSource

class BaseStorageFileMetadataBuilder(ABC):
    """
    Abstrakcyjna klasa bazowa dla builderów metadanych plików.
    Definiuje interfejs do tworzenia obiektu FileInfo na podstawie kontekstu ingestii.
    """
    @abstractmethod
    def build_file_info(
        self,
        domain_source: DomainSource,
        dataset_name: str,
        ingestion_date: str, # Format "YYYY-MM-DD"
        data_lake_storage_account_name: str,
        container_name: str,
        **kwargs: Dict[str, Any]
    ) -> FileInfo:       
        pass