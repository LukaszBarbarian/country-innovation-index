# src/common/storage_metadata_file_builder/default_storage_file_builder.py

import json
import hashlib
from typing import Dict, Any, List

from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.common.models.file_info import FileInfo
from src.common.contexts.bronze_context import BronzeContext
from src.common.models.processed_result import ProcessedResult


class DefaultStorageFileBuilder(BaseStorageFileBuilder):

    def build_file_output(self, 
                          processed_records_results: List[ProcessedResult], 
                          context: BronzeContext, 
                          storage_account_name: str, 
                          container_name: str) -> Dict[str, Any]:
        
        # 1. Dane do zapisania
        records_data_only = [res.data for res in processed_records_results]
        file_content_str = json.dumps(records_data_only, indent=2)
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        # 2. Oblicz hash payloadu (jeśli obecny)
        payload_hash = self._compute_payload_hash(context.api_request_payload)

        # 3. Generuj ścieżkę i nazwę pliku z hashem
        full_path_in_container, file_name = self._generate_blob_path_and_name(
            context, 
            file_extension="json", 
            payload_hash=payload_hash
        )

        # 4. Tagi do bloba
        blob_tags = {
            "correlationId": context.correlation_id,
            "ingestionTimestampUTC": context.ingestion_time_utc.isoformat() + "Z",
            "domainSource": context.domain_source.value,
            "datasetName": context.dataset_name,
            "recordCount": str(len(records_data_only)),
            "payloadHash": payload_hash
        }

        # 5. FileInfo
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
            blob_tags=blob_tags,
            payload_hash=payload_hash
        )

        return {
            "file_content_bytes": file_content_bytes,
            "file_info": file_info
        }

    def _compute_payload_hash(self, api_request_payload: Dict[str, Any]) -> str:
        """
        Oblicza hash zapytania na podstawie api_config_payload.
        """
        if not api_request_payload:
            return "nohash"
        normalized_str = json.dumps(api_request_payload, sort_keys=True)
        return hashlib.sha256(normalized_str.encode('utf-8')).hexdigest()[:8]

    # src/common/storage_metadata_file_builder/default_storage_file_builder.py

    def _generate_blob_path_and_name(self, context: BronzeContext, file_extension: str, payload_hash: str) -> (str, str):
        """
        Generuje ścieżkę i nazwę pliku, używając hasha payloadu,
        co zapewnia unikalność dla danego dnia i payloadu.
        """
        file_name = f"{context.dataset_name}_{context.correlation_id}_{payload_hash}.{file_extension}"
        
        blob_path = f"{context.domain_source.value}/{context.ingestion_time_utc.strftime('%Y/%m/%d')}/{file_name}"
        
        return blob_path, file_name