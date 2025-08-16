# src/bronze/ingestion/api_ingestion_strategy.py

import logging
import traceback
from typing import List, Optional, Dict, Any

from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.config.config_manager import ConfigManager
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.api_client_factory import ApiClientFactory
from src.common.factories.data_processor_factory import DataProcessorFactory
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.models.file_info import FileInfo
from src.common.models.ingestions import IngestionContext, IngestionResult
from src.common.models.processed_result import ProcessedResult
from src.common.registers.ingestion_strategy_registry import IngestionStrategyRegistry
from src.common.azure_clients.blob_client_manager import BlobClientManager

logger = logging.getLogger(__name__)

@IngestionStrategyRegistry.register(DomainSourceType.API)
class ApiIngestionStrategy(BaseIngestionStrategy):
    def __init__(self, config: ConfigManager):
        super().__init__(config)
        
    async def ingest(self, context: IngestionContext) -> IngestionResult:
        all_output_paths: List[str] = []

        source_config = context.source_config
        
        try:
            api_client = ApiClientFactory.get_instance(source_config.domain_source, config=self.config)
            data_processor = DataProcessorFactory.get_instance(source_config.domain_source)
            file_metadata_builder = StorageFileBuilderFactory.get_instance(ETLLayer.BRONZE, config=self.config)
            storage_manager = BlobClientManager(context.etl_layer.value)
            
            all_processed_records_results: List[ProcessedResult] = []

            async with api_client as client:
                fetch_results = await client.fetch_all(context)
            
            for result in fetch_results:
                for raw_record in result.data:
                    processed_record_result = data_processor.process(raw_record, context)
                    all_processed_records_results.append(processed_record_result)

            logger.info(f"Finished fetching all records for {source_config.dataset_name}. Total records: {len(all_processed_records_results)}")

            if not all_processed_records_results:
                logger.info(f"No records fetched for {source_config.dataset_name}. Skipping file save.")
                return self._create_result(
                    context=context,
                    status="COMPLETED",
                    message="No records fetched, skipping file save.")
            
            for processed_result in all_processed_records_results:
                file_output = file_metadata_builder.build_file_output(
                    processed_records_results=processed_result,
                    context=context,
                    container_name=ETLLayer.BRONZE.value
                )
                file_content_bytes = file_output["file_content_bytes"]
                file_info: FileInfo = file_output["file_info"]

                file_info.file_size_bytes = await storage_manager.upload_blob(
                    file_content_bytes=file_content_bytes,
                    file_info=file_info
                )

                if file_info.file_size_bytes > 0:
                    all_output_paths.append(file_info.full_path_in_container)

            if not all_output_paths:
                return self._create_result(
                    context=context,
                    status="SKIPPED",
                    message="No new files uploaded."
                )
            else:
                message = f"API data successfully processed and stored. Uploaded {len(all_output_paths)} files."
                return self._create_result(
                    context=context,
                    status="COMPLETED",
                    message=message,
                    output_paths=all_output_paths
                )
        
        except Exception as e:
            logger.error(f"Error in ApiIngestionStrategy for {source_config.domain_source}: {e}")
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