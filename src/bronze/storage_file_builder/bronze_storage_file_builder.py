# src/bronze/storage_file_builder/bronze_storage_file_builder.py
from dataclasses import asdict
import datetime
from enum import Enum
import hashlib
import json
from typing import Dict, Any, List, Tuple
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.models.manifest import BronzeManifestSourceConfigPayload
from src.common.enums.etl_layers import ETLLayer
from src.common.models.ingestion_result import IngestionResult
from src.common.registers.storage_file_builder_registry import StorageFileBuilderRegistry
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.common.models.file_info import FileInfo
from src.common.models.processed_result import ProcessedResult

@StorageFileBuilderRegistry.register(ETLLayer.BRONZE)
class BronzeStorageFileBuilder(BaseStorageFileBuilder):
    """
    A file builder class for the Bronze layer.

    This class is responsible for building file content and metadata for data ingested
    into the Bronze layer. It serializes data into a JSON format and constructs
    the file path and name based on the data source and ingestion details.
    """
    def build_file(self,
                   correlation_id: str, 
                   container_name: str,
                   storage_account_name: str,
                   **kwargs: Any) -> Dict[str, Any]:
        """
        Builds the file content and metadata for a single ingestion task.

        Args:
            correlation_id (str): A unique identifier for the entire pipeline run.
            container_name (str): The name of the storage container where the file will be saved.
            storage_account_name (str): The name of the storage account.
            **kwargs (Any): Additional keyword arguments, expected to contain `processed_records_results`,
                            `config_payload`, and `ingestion_date`.

        Returns:
            Dict[str, Any]: A dictionary containing the file content in bytes and a `FileInfo` object.

        Raises:
            ValueError: If `processed_records_results` is not provided.
        """
        processed_records_results: ProcessedResult = kwargs.get("processed_records_results")
        if not processed_records_results:
            raise ValueError("ProcessedResult must be provided for Bronze builder.")
        
        config_payload: BronzeManifestSourceConfigPayload = kwargs.get("config_payload")
        ingestion_date: datetime.datetime = kwargs.get("ingestion_date")

        domain_source = config_payload.domain_source
        dataset_name = config_payload.dataset_name

        records_data_only_dicts = [r.data for r in processed_records_results]

        file_content_str = json.dumps(records_data_only_dicts, indent=2, default=str)
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        hash_name = self._compute_hash_name(config_payload)
        
        file_name = f"{dataset_name}_{correlation_id}_{hash_name}.json"
        full_path_in_container = f"{domain_source.value.lower()}/{ingestion_date.strftime("%Y/%m/%d")}/{file_name}"

        full_path_url = self.build_blob_url(container_name, full_path_in_container, storage_account_name)

        file_info = FileInfo(
            container_name=container_name,
            full_path_in_container=full_path_in_container,
            file_name=file_name,
            file_size_bytes=file_size_bytes,
            domain_source=domain_source,
            dataset_name=dataset_name,
            ingestion_date=ingestion_date,
            correlation_id=correlation_id,
            hash_name=hash_name,
            full_blob_url=full_path_url
        )

        return {
            "file_content_bytes": file_content_bytes,
            "file_info": file_info
        }
    

    def _compute_hash_name(self, config_payload: BronzeManifestSourceConfigPayload) -> str:
        """
        Computes a truncated SHA256 hash of the normalized configuration payload.

        The hash is used to create a unique and reproducible file name based on
        the source configuration. This helps with idempotency and data lineage.

        Args:
            config_payload (BronzeManifestSourceConfigPayload): The configuration
                                                                object for the source.

        Returns:
            str: A truncated SHA256 hash string (first 8 characters).
        """
        payload_dict = asdict(config_payload)

        for key, value in payload_dict.items():
            if isinstance(value, Enum):
                payload_dict[key] = value.value

        normalized_str = json.dumps(payload_dict, indent=2, sort_keys=True)
        return hashlib.sha256(normalized_str.encode("utf-8")).hexdigest()[:8]

    def build_summary_file_output(self, 
                                 context: BronzeContext,
                                 results: List[IngestionResult],
                                 container_name: str,
                                 storage_account_name: str,
                                 **kwargs: Any) -> Dict[str, Any]:
        """
        Creates the content and metadata for an orchestration summary file.

        This method compiles a summary of the entire orchestration run, including
        the overall status, duration, and detailed results for each ingested source.
        The summary is formatted as a JSON string.

        Args:
            context (BronzeContext): The context object for the pipeline run.
            results (List[IngestionResult]): A list of results from each ingestion task.
            container_name (str): The name of the storage container for the summary file.
            storage_account_name (str): The name of the storage account.
            **kwargs (Any): Additional keyword arguments, expected to contain `duration_orchestrator`.

        Returns:
            Dict[str, Any]: A dictionary containing the summary file content in bytes
                            and a `FileInfo` object.
        """
        # Generate summary content
        summary_data = {
            "status": "COMPLETED" if all(r.status in ["COMPLETED", "SKIPPED"] for r in results) else "FAILED",
            "env": context.env.value,
            "etl_layer": context.etl_layer.value,
            "correlation_id": context.correlation_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "processed_items": len(results),
            "duration_in_ms" : kwargs.get("duration_orchestrator"),
            "results": [r.to_dict() for r in results]
        }
        
        file_content_str = json.dumps(summary_data, indent=4)
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        file_name = f"ingestion_summary__{context.correlation_id}.json"
        
        # Create file path: outputs/ingestion_summaries/file_name
        blob_path = f"outputs/ingestion_summaries/{file_name}"
        full_blob_url = self.build_blob_url(container_name, blob_path, storage_account_name)


        file_info = FileInfo(
            container_name=container_name,
            full_path_in_container=blob_path,
            file_name=file_name,
            file_size_bytes=file_size_bytes,
            domain_source=None,
            dataset_name=None,
            ingestion_date=datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
            correlation_id=context.correlation_id,
            hash_name=context.correlation_id,
            full_blob_url=full_blob_url
        )

        return {
            "file_content_bytes": file_content_bytes,
            "file_info": file_info
        }