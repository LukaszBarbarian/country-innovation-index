# src/silver/storage_file_builder/silver_storage_file_builder.py
from typing import Dict, Any, Optional
from src.common.models.base_context import BaseContext
from src.common.enums.etl_layers import ETLLayer
from src.common.registers.storage_file_builder_registry import StorageFileBuilderRegistry
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.common.config.config_manager import ConfigManager

@StorageFileBuilderRegistry.register(ETLLayer.SILVER)
class SilverStorageFileBuilder(BaseStorageFileBuilder):
    
    def __init__(self, config: ConfigManager, context: BaseContext):
        super().__init__(config)
        self.context = context

    def get_delta_table_path(self, container_name: str, model_type: str) -> str:
        ingestion_date_path = self._get_ingestion_date_path(self.context.ingestion_time_utc)
        return f"{model_type.lower()}/{ingestion_date_path}"

    def build_file_output(self,
                          context: BaseContext,
                          container_name: str,
                          **kwargs: Any) -> Dict[str, Any]:
        """
        Nadpisana metoda do generowania ścieżek dla warstwy Silver.
        Zwraca tylko ścieżkę, ponieważ Spark zajmuje się resztą.
        """

        model_type = kwargs.get("model_type")
        if not model_type:
            raise ValueError("Model type must be provided for Silver builder.")

        output_path = self.get_delta_table_path(container_name, model_type)
        full_path_url = self.build_blob_url(container_name, output_path, self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME"))

        
        return {
            "output_path": output_path,
            "full_path_url" : full_path_url
        }