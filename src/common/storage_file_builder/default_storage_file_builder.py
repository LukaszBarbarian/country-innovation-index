# src/common/storage_metadata_file_builder/default_storage_metadata_file_builder.py
import json
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.common.models.file_info import FileInfo
from src.common.contexts.bronze_context import BronzeContext
from typing import Dict, Any, List
from datetime import datetime
from src.common.models.processed_result import ProcessedResult


class DefaultStorageFileBuilder(BaseStorageFileBuilder):
    def build_file_output(self, 
                          processed_records_results: List[ProcessedResult], 
                          context: BronzeContext, 
                          storage_account_name: str, 
                          container_name: str) -> Dict[str, Any]:
        
        # 1. Wyodrębnij słowniki danych z obiektów ProcessedResult
        records_data_only = [res.data for res in processed_records_results]

        # 2. Serializuj listę rekordów bezpośrednio do JSON-a
        # NIE dodajemy sekcji "metadata" do samego pliku JSON
        file_content_str = json.dumps(records_data_only, indent=2) # Plik to teraz po prostu lista rekordów
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        # 3. Generuj ścieżkę i nazwę pliku (używamy 'json' jako rozszerzenia)
        full_path_in_container, file_name = self._generate_blob_path_and_name(context, file_extension="json")

        # 4. Przygotuj tagi bloba
        blob_tags = {
            "correlationId": context.correlation_id,
            "ingestionTimestampUTC": context.ingestion_time_utc.isoformat() + "Z",
            "domainSource": context.domain_source.value,
            "datasetName": context.dataset_name,
            "recordCount": str(len(records_data_only)) 
        }

        # 5. Buduj FileInfo
        file_info = FileInfo(
            container_name=container_name,
            full_path_in_container=full_path_in_container,
            file_name=file_name,
            storage_account_name=storage_account_name,
            file_size_bytes=file_size_bytes,
            domain_source=context.domain_source.value,
            dataset_name=context.dataset_name,
            ingestion_date=context.ingestion_time_utc.strftime("%Y-%m-%d"),
            correlation_id=context.correlation_id,
            blob_tags=blob_tags # Dodajemy tagi bloba do FileInfo
        )

        return {
            "file_content_bytes": file_content_bytes,
            "file_info": file_info
        }
