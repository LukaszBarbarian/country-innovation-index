# src/silver/storage_file_builder/silver_storage_file_builder.py
from dataclasses import asdict
import datetime
import json
import os
from typing import Dict, Any, List

from src.common.enums.etl_layers import ETLLayer
from src.common.models.file_info import FileInfo
from src.common.models.process_model_result import ProcessModelResult
from src.common.registers.storage_file_builder_registry import StorageFileBuilderRegistry
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.silver.context.silver_context import SilverContext
from src.silver.models.process_model import SilverProcessModel


@StorageFileBuilderRegistry.register(ETLLayer.SILVER)
class SilverStorageFileBuilder(BaseStorageFileBuilder):
    """
    A builder for creating file metadata and content for the Silver ETL layer.
    
    This class is responsible for generating the correct file paths and
    metadata for both the output Delta tables and the final summary file.
    It registers itself for the 'silver' ETL layer.
    """
    
    def _generate_delta_table_path(self, container_name: str, model_name: str, storage_account_name: str) -> str:
        """
        Generates a standard path to a Delta table, e.g., /silver/countries.
        Paths are defined in the configuration, allowing for easy changes without
        code modification.
        """
        return f"abfss://{container_name}@{storage_account_name.lower()}.dfs.core.windows.net/{model_name}"

    def build_file(self,
                   context: SilverContext,
                   container_name: str,
                   storage_account_name: str,
                   **kwargs: Any) -> Dict[str, Any]:
        """
        Builds the output metadata for a Delta table.
        
        Args:
            context (SilverContext): The Silver ETL context.
            container_name (str): The name of the storage container.
            storage_account_name (str): The name of the storage account.
            **kwargs (Any): Additional keyword arguments, including the 'model'.

        Returns:
            Dict[str, Any]: A dictionary containing a `FileInfo` object.
        """
        model: SilverProcessModel = kwargs.get("model")
        if not model:
            raise ValueError("Model name must be provided for Silver builder.")

        # Generate the path to the table
        full_path_abfss = self._generate_delta_table_path(container_name=container_name, model_name=model.name, storage_account_name=storage_account_name)
        
        # Delta file metadata (versioning and partitioning are managed by Spark)
        file_info = FileInfo(
            container_name=container_name,
            full_path_in_container=full_path_abfss,
            file_name=f"{model.name}.delta",
            file_size_bytes=0,  # Spark manages the size
            domain_source=None,
            dataset_name=None,
            ingestion_date=None,
            correlation_id=context.correlation_id,
            blob_tags={
                "etlLayer": ETLLayer.SILVER.value,
                "modelName": model.name,
                "correlationId": context.correlation_id,
            },
            hash_name=None,
            full_blob_url=full_path_abfss
        )

        return {
            "file_info": file_info
        }

    def build_summary_file_output(self,
                                  context: SilverContext,
                                  results: List[ProcessModelResult],
                                  container_name: str,
                                  storage_account_name: str,
                                  **kwargs: Any) -> Dict[str, Any]:
        """
        Creates the content and metadata for the Silver layer's summary file.
        
        This method compiles a list of all processed model results and
        generates a JSON file with a comprehensive summary of the ETL run.

        Args:
            context (SilverContext): The Silver ETL context.
            results (List[ProcessModelResult]): The list of results for each processed model.
            container_name (str): The name of the storage container.
            storage_account_name (str): The name of the storage account.
            **kwargs (Any): Additional keyword arguments, including the total orchestration duration.
            
        Returns:
            Dict[str, Any]: A dictionary containing the summary file's content
                            (as bytes) and its `FileInfo` object.
        """
        # Generate summary content
        processed_results = []
        for r in results:
            # Manually create a dictionary with the desired structure
            result_dict = {
                "model": r.model,
                "status": r.status,
                "output_path": r.output_path,
                "correlation_id": r.correlation_id,
                "operation_type": r.operation_type,
                "record_count": r.record_count,
                "error_details": r.error_details,
                "duration_in_ms": r.duration_in_ms
            }
            processed_results.append(result_dict)

        # Generate the summary content
        summary_data = {
            "status": "COMPLETED" if all(r.status == "COMPLETED" for r in results) else "FAILED",
            "env": context.env.value,
            "etl_layer": context.etl_layer.value,
            "correlation_id": context.correlation_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "processed_models": len(results),
            "duration_in_ms": kwargs.get("duration_orchestrator"),
            "results": processed_results # Use the manually processed list
        }
        
        file_content_str = json.dumps(summary_data, indent=4, default=str)
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        file_name = f"processing_summary__{context.correlation_id}.json"
        blob_path = f"outputs/summaries/{file_name}"
        full_blob_url = self.build_blob_url(container_name, blob_path, storage_account_name)

        blob_tags = {
            "correlationId": context.correlation_id,
            "ingestionTimestampUTC": datetime.datetime.utcnow().isoformat() + "Z",
            "type": "goldSummary",
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