import logging
import traceback
from typing import List, Optional, Any, Dict

from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.azure_clients.blob_client_manager import BlobClientManager
from src.common.config.config_manager import ConfigManager
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.models.ingestion_context import IngestionContext
from src.common.models.ingestion_result import IngestionResult
from src.common.registers.ingestion_strategy_registry import IngestionStrategyRegistry

logger = logging.getLogger(__name__)

@IngestionStrategyRegistry.register(DomainSourceType.STATIC_FILE)
class StaticFileIngestionStrategy(BaseIngestionStrategy):
    def __init__(self, config: ConfigManager):
        super().__init__(config)


    async def ingest(self, context: IngestionContext) -> IngestionResult:
        self.file_builder = StorageFileBuilderFactory.get_instance(ETLLayer.BRONZE, config=self.config)

        storage_manager = BlobClientManager(context.etl_layer.value)

        file_paths = context.source_config.request_payload.file_path

        try:
            if not file_paths:
                logger.error(
                    f"Missing file_path in context for STATIC_FILE type. "
                    f"Correlation ID: {context.correlation_id}"
                )
                return self._create_result(
                    context=context,
                    status="FAILED",
                    message="File path not provided in payload.",
                    error_details={"error": "file_path is required for STATIC_FILE type"}
                )
            
            account_name = self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")
            container_name = ETLLayer.BRONZE.value

            full_urls = []
            for path in file_paths:
                properties = await storage_manager.get_blob_properties(path)


                blob_url = self.file_builder.build_blob_url(container_name, path, account_name)
                
                logger.debug(f"Converted blob path '{path}' to URL '{blob_url}'")
                full_urls.append(blob_url)

            return self._create_result(
                context=context,
                status="COMPLETED",
                message="Static file path(s) converted to blob URLs.",
                output_paths=full_urls
            )

        except Exception as e:
            logger.error(f"Error in StaticFileIngestionStrategy: {e}")
            error_details = {
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc()
            }
            return self._create_result(
                context=context,
                status="FAILED",
                message=f"Ingestion failed: {str(e)}",
                error_details=error_details
            )

    def _create_result(
        self,
        context: IngestionContext,
        status: str,
        message: str,
        output_paths: Optional[List[str]] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> IngestionResult:
        """
        Metoda pomocnicza do tworzenia obiektu IngestionResult, 
        dostosowana do BronzeLayerContext.
        """
        return IngestionResult(
            correlation_id=context.correlation_id,
            env=context.env.value,
            etl_layer=context.etl_layer.value,
            domain_source=context.source_config.domain_source,
            domain_source_type=context.source_config.domain_source_type,
            dataset_name=context.source_config.dataset_name,
            status=status,
            message=message,
            output_paths=output_paths,
            error_details=error_details or {}
        )