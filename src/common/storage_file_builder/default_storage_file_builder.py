# src/common/storage_metadata_file_builder/default_storage_file_builder.py

import json
import hashlib
from typing import Dict, Any, List, cast
from xml import dom

from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.common.models.file_info import FileInfo
from src.common.models.processed_result import ProcessedResult


class DefaultStorageFileBuilder(BaseStorageFileBuilder):

    def build_file_output(self, 
                          processed_records_results: ProcessedResult, 
                          context: BaseLayerContext, 
                          container_name: str) -> Dict[str, Any]:

        # 1. Dane do zapisania
        records_data_only = processed_records_results.data
        file_content_str = json.dumps(records_data_only, indent=2)
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        # 2. Oblicz hash payloadu (jeśli obecny)
        request_config_payload = context.payload.get("source_config_payload", {})
        
        if not request_config_payload:
            raise

        payload_hash = self._compute_payload_hash(request_config_payload)

        # 3. Generuj ścieżkę i nazwę pliku z hashem
        full_path_in_container, file_name = self._generate_blob_path_and_name(dataset_name=context.dataset_name, 
                                          correlation_id=context.correlation_id, 
                                          domain_source=context.domain_source.value,
                                          file_extension="json",
                                          ingestion_time_utc=context.ingestion_time_utc.strftime('%Y/%m/%d'),
                                          payload_hash=payload_hash)

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