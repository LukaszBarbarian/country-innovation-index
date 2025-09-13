# src/silver/persisters/model_persister.py

import datetime
import logging
from turtle import mode
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase
from src.common.enums.etl_layers import ETLLayer
from src.common.models.base_process import BaseProcessModel
from src.common.models.file_info import FileInfo
from src.common.models.process_model_result import ProcessModelResult
from src.common.spark.spark_service import SparkService
from typing import Optional

logger = logging.getLogger(__name__)

class ModelPersister:
    """
    Handles the persistence of a Spark DataFrame to Azure Data Lake Storage as a Delta table.
    It uses a factory to determine the correct file path based on the ETL layer.
    """
    def __init__(self, 
                 layer: ETLLayer,
                 config: ConfigManager, 
                 context: ContextBase, 
                 spark: SparkService):
        """
        Initializes the ModelPersister.

        Args:
            layer (ETLLayer): The ETL layer (e.g., Bronze, Silver, Gold) to which the data belongs.
            config (ConfigManager): The configuration manager instance.
            context (ContextBase): The context object for the current process.
            spark (SparkService): The Spark service instance for interacting with Spark.
        """
        self.config = config
        self.context = context
        self.spark = spark
        self.layer = layer
        self.storage_account_name = self.config.get("DATA_LAKE_STORAGE_ACCOUNT_NAME")


    def persist_model(self, model: BaseProcessModel) -> Optional[ProcessModelResult]:
        """
        Persists a Spark DataFrame as a Delta table.

        The method first checks if the DataFrame is empty. If it is, persistence is skipped.
        It then uses the `StorageFileBuilderFactory` to dynamically determine the output path
        and finally writes the DataFrame to that path using Delta format.

        Args:
            model (BaseProcessModel): The model object containing the DataFrame and metadata to be persisted.

        Returns:
            Optional[ProcessModelResult]: A result object containing information about the persistence operation,
                                          or None if the operation is skipped.
        """
        if model.data.isEmpty():
            logger.warning(f"Model '{model.name}' is empty, skipping persistence.")
            return ProcessModelResult(
                model=model,
                status="SKIPPED",
                output_path="N/A",
                correlation_id=self.context.correlation_id,
                operation_type="N/A",
                record_count=0
            )
        
        file_builder = StorageFileBuilderFactory.get_instance(self.layer, config=self.config)

        output_info = file_builder.build_file(
            context=self.context,
            container_name=self.context.etl_layer.value.lower(), 
            storage_account_name=self.storage_account_name,
            model=model
        )

        file_info: FileInfo = output_info.get("file_info")
        
        # We use Spark to write the data
        self.spark.write_delta(
            df=model.data,
            abfss_url=file_info.full_path_in_container,
            partition_cols=model.partition_cols,
            options={"mergeSchema": "true"} 
        )
        
        logger.info(f"Successfully persisted '{model.name}' model to {file_info.full_path_in_container}")
        
        # Return the full result
        return ProcessModelResult(
            model=model.name,
            status="COMPLETED",
            output_path=file_info.full_path_in_container,
            correlation_id=self.context.correlation_id,
            operation_type="UPSERT",
            record_count=model.data.count()
        )