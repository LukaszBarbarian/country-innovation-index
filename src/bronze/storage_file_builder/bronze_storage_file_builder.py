# src/bronze/storage_file_builder/bronze_storage_file_builder.py
from dataclasses import asdict
import datetime
import hashlib
import json
from typing import Dict, Any, List, Tuple
from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.enums.etl_layers import ETLLayer
from src.common.models.ingestions import IngestionContext, IngestionResult
from src.common.registers.storage_file_builder_registry import StorageFileBuilderRegistry
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.common.models.file_info import FileInfo
from src.common.models.processed_result import ProcessedResult

@StorageFileBuilderRegistry.register(ETLLayer.BRONZE)
class BronzeStorageFileBuilder(BaseStorageFileBuilder):

    def _generate_blob_path_and_name(self,
                                     context: IngestionContext,
                                     hash_name: str) -> Tuple[str, str]:
        ingestion_date_path = self._get_ingestion_date_path(context.ingestion_time_utc)
        file_name = f"{context.source_config.dataset_name}_{context.correlation_id}_{hash_name}.json"
        full_path_in_container = f"{context.source_config.domain_source.value}/{ingestion_date_path}/{file_name}"
        return full_path_in_container, file_name

    def build_file_output(self,
                          context: IngestionContext,
                          container_name: str,
                          **kwargs: Any) -> Dict[str, Any]:
        
        processed_records_results: ProcessedResult = kwargs.get("processed_records_results")
        if not processed_records_results:
            raise ValueError("ProcessedResult must be provided for Bronze builder.")

        records_data_only = processed_records_results.data
        file_content_str = json.dumps(records_data_only, indent=2)
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        hash_name = self._compute_hash_name(api_request_payload=context.source_config)
        
        full_path_in_container, file_name = self._generate_blob_path_and_name(context, hash_name)
        full_path_url = self.build_blob_url(container_name, full_path_in_container, self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME"))

        blob_tags = {
            "correlationId": context.correlation_id,
            "ingestionTimestampUTC": context.ingestion_time_utc.isoformat() + "Z",
            "domainSource": context.source_config.domain_source,
            "datasetName": context.source_config.dataset_name,
            "recordCount": str(len(records_data_only)),
            "payloadHash": hash_name
        }

        file_info = FileInfo(
            container_name=container_name,
            full_path_in_container=full_path_in_container,
            file_name=file_name,
            file_size_bytes=file_size_bytes,
            domain_source=context.source_config.domain_source,
            dataset_name=context.source_config.dataset_name,
            ingestion_date=context.ingestion_time_utc.strftime("%Y-%m-%d"),
            correlation_id=context.correlation_id,
            blob_tags=blob_tags,
            hash_name=hash_name,
            full_blob_url=full_path_url
        )

        return {
            "file_content_bytes": file_content_bytes,
            "file_info": file_info
        }
    




    def build_summary_file_output(self,
                                context: IngestionContext,
                                ingestion_results: List[IngestionResult],
                                container_name: str) -> Dict[str, Any]:
        """
        Tworzy zawartość i metadane dla pliku podsumowującego orkiestrację.
        """
        # Generowanie zawartości podsumowania
        summary_data = {
            "status": "BRONZE_COMPLETED" if all(r.status in ["COMPLETED", "SKIPPED"] for r in ingestion_results) else "BRONZE_FAILED",
            "env": context.env.value,
            "layer_name": context.etl_layer.value,
            "correlation_id": context.correlation_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "processed_items": len(ingestion_results),
            "results": [r.to_dict() for r in ingestion_results]
        }
        
        file_content_str = json.dumps(summary_data, indent=4)
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        file_name = f"ingestion_summary__{context.correlation_id}.json"
        
        # Tworzenie ścieżki pliku: output/ingestion_summaries/nazwa_pliku
        blob_path = f"outputs/ingestion_summaries/{file_name}"
        full_blob_url = self.build_blob_url(container_name, blob_path, self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME"))

        blob_tags = {
            "correlationId": context.correlation_id,
            "ingestionTimestampUTC": datetime.datetime.utcnow().isoformat() + "Z",
            "type": "summary",
            "status": summary_data["status"],
        }
        
        file_info = FileInfo(
            container_name=container_name,
            full_path_in_container=blob_path,
            file_name=file_name,
            file_size_bytes=file_size_bytes,
            domain_source=None,
            dataset_name=None,
            ingestion_date=datetime.datetime.utcnow().strftime("%Y-%m-%d"),
            correlation_id=context.correlation_id,
            blob_tags=blob_tags,
            hash_name=context.correlation_id,
            full_blob_url=full_blob_url
        )

        return {
            "file_content_bytes": file_content_bytes,
            "file_info": file_info
        }
