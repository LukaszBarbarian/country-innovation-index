import logging
from typing import List
from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.models.ingestion_result import IngestionResult
from src.common.registers.ingestion_strategy_registry import IngestionStrategyRegistry
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder

logger = logging.getLogger(__name__)

@IngestionStrategyRegistry.register(DomainSourceType.STATIC_FILE)
class StaticFileIngestionStrategy(BaseIngestionStrategy):
    def __init__(self, config):
        super().__init__(config)

    async def ingest(self, context: BronzeLayerContext) -> IngestionResult:
        self.file_builder = StorageFileBuilderFactory.get_instance(context.domain_source, config=self.config)

        if not context.file_paths:
            logger.error(
                f"Missing file_path in context for STATIC_FILE type. "
                f"Correlation ID: {context.correlation_id}"
            )
            return self.create_result(
                status="FAILED",
                message="File path not provided in payload.",
                error_details={"error": "file_path is required for STATIC_FILE type"}
            )
        
        account_name = self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")
        container_name = ETLLayer.BRONZE.value

        full_urls = []
        for path in context.file_paths:
            blob_url = self.file_builder.build_blob_url(container_name, path, account_name)
            logger.debug(f"Converted blob path '{path}' to URL '{blob_url}'")
            full_urls.append(blob_url)

        return self.create_result(
            status="COMPLETED",
            message="Static file path(s) converted to blob URLs.",
            source_response_status_code=200,
            output_paths=full_urls
        )
