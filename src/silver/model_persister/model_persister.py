# src/silver/persisters/model_persister.py

import logging
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.models.etl_model import EtlModel
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import BaseContext
from src.common.enums.etl_layers import ETLLayer
from src.common.spark.spark_service import SparkService

logger = logging.getLogger(__name__)

class ModelPersister:
    def __init__(self, config: ConfigManager, context: BaseContext, spark: SparkService):
        self.config = config
        self.context = context
        self.spark = spark

    def persist_model(self, model: EtlModel) -> str:
        """
        Persists a Spark DataFrame as a Delta table for the Silver layer.
        """
        # Validate that the model is not empty
        if model.data.isEmpty():
            logger.warning(f"Model '{model.model_type.name}' is empty, skipping persistence.")
            return None
        
        # Build the path for the Delta table using the dedicated builder
        file_builder = StorageFileBuilderFactory.get_instance(ETLLayer.SILVER, config=self.config, context=self.context)        
        output_info = file_builder.build_file_output(
            context=self.context,
            container_name=self.context.etl_layer.name,
            model_type=model.model_type.name
        )

        full_path_url = output_info.get("full_path_url")

        # Use Spark to write the DataFrame to the Delta table
        self.spark.write_json(model.data, full_path_url)        
        
        full_blob_url = output_info.get("full_blob_url")
        logger.info(f"Successfully persisted '{model.model_type.name}' model to {full_blob_url}")
        
        return full_blob_url