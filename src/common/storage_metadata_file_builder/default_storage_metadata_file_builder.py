# src/common/file_metadata/default_file_metadata_builder.py
import uuid
from datetime import datetime
from typing import Dict, Any

# Pamiętaj, aby ścieżka importu była zgodna z Twoją strukturą katalogów
from src.common.storage_metadata_file_builder.base_storage_path_builder import BaseStorageFileMetadataBuilder
from src.common.models.file_info import FileInfo
from src.common.enums.domain_source import DomainSource # Jeśli DomainSource to Enum

class DefaultStorageFileMetadataBuilder(BaseStorageFileMetadataBuilder):
    """
    Domyślny builder metadanych plików.
    Tworzy standardową ścieżkę i nazwę pliku, jeśli nie ma specyficznego buildera dla danego źródła.
    Format ścieżki: <domain_source_lower>/<dataset_name_lower>/<ingestion_date>/<dataset_name_lower>_<timestamp>_<uuid>.json
    """
    def build_file_info(
        self,
        domain_source: DomainSource, # Użyj typu DomainSource, jeśli jest to Enum
        dataset_name: str,
        ingestion_date: str, # Format "YYYY-MM-DD"
        data_lake_storage_account_name: str,
        bronze_container_name: str,
        **kwargs: Dict[str, Any]
    ) -> FileInfo:
        """
        Implementuje domyślną logikę budowania metadanych pliku.
        """
        # --- 1. Budowanie ścieżki katalogu w kontenerze ---
        # Konwersja domain_source na string i formatowanie (np. z Enum.VALUE na "value")
        # Zakładamy, że domain_source jest obiektem z atrybutem .value (jeśli to Enum)
        source_segment = domain_source.value.lower().replace(" ", "_") if hasattr(domain_source, 'value') else str(domain_source).lower().replace(" ", "_")
        dataset_segment = dataset_name.lower().replace(" ", "_")

        # Możesz dodać logikę partycjonowania z kwargs tutaj, jeśli ma być domyślna
        additional_path_segments = []
        # Przykładowo, jeśli kwargs zawiera "year" i "month" i chcesz je w ścieżce
        # if 'year' in kwargs and 'month' in kwargs:
        #     additional_path_segments.append(f"year={kwargs['year']}")
        #     additional_path_segments.append(f"month={kwargs['month']}")

        base_blob_dir_path_in_container_parts = [
            source_segment,
            dataset_segment,
            ingestion_date,
            *additional_path_segments # Rozpakowanie dodatkowych segmentów
        ]
        # Usuń puste segmenty, jeśli jakieś się pojawią (np. jeśli additional_path_segments jest puste)
        base_blob_dir_path_in_container = "/".join(filter(None, base_blob_dir_path_in_container_parts))

        # --- 2. Generowanie nazwy pliku ---
        file_name_prefix = dataset_segment
        file_name_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        file_name_uuid_suffix = uuid.uuid4().hex[:8] # Krótki unikalny sufiks

        # Możesz dodać elementy z kwargs do nazwy pliku, jeśli mają być domyślne
        additional_file_name_parts = []
        # Przykładowo, jeśli kwargs zawiera "report_id"
        # if 'report_id' in kwargs:
        #     additional_file_name_parts.append(f"report_{kwargs['report_id']}")

        file_name = f"{file_name_prefix}_{'_'.join(additional_file_name_parts) if additional_file_name_parts else ''}{file_name_timestamp}_{file_name_uuid_suffix}.json"
        # Usuń podwójne podkreślenia, jeśli additional_file_name_parts było puste
        file_name = file_name.replace('__', '_')
        # Usuń podkreślenie na początku, jeśli timestamp zaczynałby się od niego
        if file_name.startswith('_'):
             file_name = file_name[1:]


        # --- 3. Składanie pełnej ścieżki do pliku w kontenerze ---
        full_file_path_in_container = f"{base_blob_dir_path_in_container}/{file_name}"

        # --- 4. Tworzenie i zwracanie obiektu FileInfo ---
        file_info = FileInfo(
            container_name=bronze_container_name,
            full_path_in_container=full_file_path_in_container,
            file_name=file_name,
            storage_account_name=data_lake_storage_account_name
            # file_size_bytes będzie uzupełniony w DataIngestor po zapisie
        )
        return file_info