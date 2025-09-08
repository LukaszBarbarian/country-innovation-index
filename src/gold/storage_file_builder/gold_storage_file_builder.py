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
from src.gold.contexts.gold_layer_context import GoldContext
from src.gold.models.process_model import GoldProcessModel

@StorageFileBuilderRegistry.register(ETLLayer.GOLD)
class GoldStorageFileBuilder(BaseStorageFileBuilder):
    
    def _generate_delta_table_path(self, container_name: str, model_name: str, storage_account_name: str) -> str:
        """
        Generuje standardową ścieżkę do tabeli Delta, np. /silver/countries.
        Ścieżki są zdefiniowane w konfiguracji, co pozwala na łatwą zmianę bez
        modyfikacji kodu.
        """
        
        return f"abfss://{container_name}@{storage_account_name.lower()}.dfs.core.windows.net/{model_name}"

    def build_file(self,
                          context: GoldContext,
                          container_name: str,
                          storage_account_name: str,
                          **kwargs: Any) -> Dict[str, Any]:
        """
        Buduje metadane wyjściowe dla tabeli Delta.
        Zwraca pełną ścieżkę i metadane pliku Delta.
        """
        model: GoldProcessModel = kwargs.get("model")
        if not model:
            raise ValueError("Model name must be provided for Silver builder.")

        # Generowanie ścieżki do tabeli
        full_path_abfss = self._generate_delta_table_path(container_name=container_name, model_name=model.name, storage_account_name=storage_account_name)
        
        # Metadane pliku Delta (wersjonowanie i partycjonowanie są zarządzane przez Spark)
        file_info = FileInfo(
            container_name=container_name,
            full_path_in_container=full_path_abfss,
            file_name=f"{model.name}.delta",
            file_size_bytes=0,  # Spark zarządza rozmiarem
            domain_source=None,
            dataset_name=None,
            correlation_id=context.correlation_id,
            blob_tags={
                "etlLayer": ETLLayer.GOLD.value,
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
                                  context: GoldContext,
                                  results: List[ProcessModelResult],
                                  container_name: str,
                                  storage_account_name: str,
                                  **kwargs: Any) -> Dict[str, Any]:
        """
        Tworzy zawartość i metadane dla pliku podsumowującego procesy w warstwie Silver.
        """
        # Generowanie zawartości podsumowania
        processed_results = []
        for r in results:
            # Ręcznie tworzymy słownik z pożądaną strukturą
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

        # Generowanie zawartości podsumowania
        summary_data = {
            "status": "COMPLETED" if all(r.status == "COMPLETED" for r in results) else "FAILED",
            "env": context.env.value,
            "etl_layer": "gold",
            "correlation_id": context.correlation_id,
            "processed_models": len(results),
            "duration_in_ms" : kwargs.get("duration_orchestrator"),
            "results": processed_results # Używamy ręcznie przetworzonej listy
        }
        
        file_content_str = json.dumps(summary_data, indent=4, default=str)
        file_content_bytes = file_content_str.encode('utf-8')
        file_size_bytes = len(file_content_bytes)

        file_name = f"processing_summary__{context.correlation_id}.json"
        blob_path = f"outputs/summaries/{file_name}"
        full_blob_url = self.build_blob_url(container_name, blob_path, storage_account_name)

        
        file_info = FileInfo(
            container_name=container_name,
            full_path_in_container=blob_path,
            file_name=file_name,
            file_size_bytes=file_size_bytes,
            domain_source=None,
            dataset_name=None,
            correlation_id=context.correlation_id,
            blob_tags=None,
            hash_name=context.correlation_id,
            full_blob_url=full_blob_url
        )

        return {
            "file_content_bytes": file_content_bytes,
            "file_info": file_info
        }