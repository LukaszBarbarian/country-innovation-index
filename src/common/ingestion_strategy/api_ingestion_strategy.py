# src/bronze/ingestion/api_ingestion_strategy.py

import logging
import traceback
from typing import List, Optional

from src.common.config.config_manager import ConfigManager
from src.common.contexts.layer_context import LayerContext
from src.common.enums.domain_source_type import DomainSourceType
from src.common.factories.api_client_factory import ApiClientFactory
from src.common.factories.data_processor_factory import DataProcessorFactory
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.models.file_info import FileInfo
from src.common.models.ingestion_result import IngestionResult
from src.common.models.processed_result import ProcessedResult
from src.common.registers.ingestion_strategy_registry import IngestionStrategyRegistry
from src.common.storage_account.blob_storage_manager import BlobStorageManager

logger = logging.getLogger(__name__)

@IngestionStrategyRegistry.register(DomainSourceType.API)
class ApiIngestionStrategy(BaseIngestionStrategy):
    async def ingest(self, context: LayerContext) -> IngestionResult:
        all_output_paths: List[str] = []
        source_response_status_code: Optional[int] = None
        
        try:
            api_client = ApiClientFactory.get_instance(context.domain_source, config=self.config)
            data_processor = DataProcessorFactory.get_instance(context.domain_source)
            file_metadata_builder = StorageFileBuilderFactory.get_instance(context.domain_source)
            storage_manager = BlobStorageManager(context.etl_layer.value)
            
            all_processed_records_results: List[ProcessedResult] = []

            async with api_client as client:
                fetch_results = await client.fetch_all(context)
            
            for result in fetch_results:
                # Tutaj możesz pobrać kod statusu z odpowiedzi API, jeśli jest dostępny.
                # source_response_status_code = result.status_code 
                for raw_record in result.data:
                    processed_record_result = data_processor.process(raw_record, context)
                    all_processed_records_results.append(processed_record_result)

            logger.info(f"Finished fetching all records for {context.dataset_name}. Total records: {len(all_processed_records_results)}")

            if not all_processed_records_results:
                logger.info(f"No records fetched for {context.dataset_name}. Skipping file save.")
                return self.create_result(
                    status="COMPLETED",
                    message="No records fetched, skipping file save.",
                    source_response_status_code=source_response_status_code
                )
            
            for processed_result in all_processed_records_results:
                # W tym miejscu dodaj brakujący argument 'storage_account_name'
                file_output = file_metadata_builder.build_file_output(
                    processed_records_results=processed_result,
                    context=context,
                    container_name=storage_manager.container_name
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
                return self.create_result(
                    status="SKIPPED",
                    message="No new files uploaded.",
                    source_response_status_code=source_response_status_code
                )
            else:
                message = f"API data successfully processed and stored. Uploaded {len(all_output_paths)} files."
                return self.create_result(
                    status="COMPLETED",
                    message=message,
                    output_paths=all_output_paths,
                    source_response_status_code=source_response_status_code
                )
        
        except Exception as e:
            logger.error(f"Error in ApiIngestionStrategy for {context.domain_source.value}: {e}")
            error_details = {
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc()
            }
            return self.create_result(
                status="FAILED",
                message=f"Ingestion failed: {str(e)}",
                error_details=error_details
            )