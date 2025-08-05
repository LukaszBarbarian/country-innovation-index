# src/ingestion/data_ingestor.py

import logging
from src.bronze.contexts.bronze_context import BronzeContext
from src.common.factories.api_client_factory import ApiClientFactory
from src.common.factories.data_processor_factory import DataProcessorFactory
from src.bronze.storage_manager.bronze_storage_manager import BronzeStorageManager
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.models.file_info import FileInfo 
from src.common.models.processed_result import ProcessedResult 
from typing import List, Optional
import traceback 

import src.bronze.init.bronze_init 

logger = logging.getLogger(__name__)

class BronzeOrchestrator(BaseOrchestrator):
    def __init__(self, config):
        super().__init__(config)
         
        self.storage_manager = BronzeStorageManager()

    async def run(self, context: BronzeContext) -> OrchestratorResult:
        logger.info(f"Starting ingestion for Dataset: {context.dataset_name} using API identified as: {context.domain_source} with CorrelationId: {context.correlation_id}")

        final_output_path: Optional[str] = None 
        api_response_status_code: Optional[int] = None


        api_client = ApiClientFactory.get_instance(context.domain_source, self.config)
        data_processor = DataProcessorFactory.get_instance(context.domain_source)

        all_processed_records_results: List[ProcessedResult] = [] 

        try:
            async with api_client as client:
                results = await client.fetch_all(context.api_request_payload)
    
                for result in results:
                    for raw_record in result.records:
                        processed_record_result = data_processor.process(raw_record, context)
                        all_processed_records_results.append(processed_record_result)
                    
            logger.info(f"Finished fetching all records for {context.dataset_name}. Total records fetched: {len(all_processed_records_results)}")

            if not all_processed_records_results:
                logger.info(f"No records fetched for {context.dataset_name} with CorrelationId: {context.correlation_id}. Skipping file save and Event Grid notification.")
                
                return OrchestratorResult(
                    status="COMPLETED",
                    correlation_id=context.correlation_id,
                    queue_message_id=context.queue_message_id,
                    api_name=context.api_name_str,
                    dataset_name=context.dataset_name,
                    layer_name=context.etl_layer.value,
                    env=context.env,
                    message="No records fetched, skipping file save.",
                    output_path=None,
                    api_response_status_code=context.api_response_status_code
                )

            file_metadata_builder = StorageFileBuilderFactory.get_instance(context.domain_source)
            
            file_output = file_metadata_builder.build_file_output(
                processed_records_results=all_processed_records_results, 
                context=context,
                storage_account_name=self.storage_account_name,
                container_name=self.storage_manager.DEFAULT_CONTAINER_NAME
            )
            file_content_bytes = file_output["file_content_bytes"]
            file_info: FileInfo = file_output["file_info"]

            file_info.file_size_bytes = await self.storage_manager.upload_file(
                file_content_bytes=file_content_bytes,
                file_info=file_info
            )
       

            final_output_path = file_info.url 
            api_response_status_code = context.api_response_status_code

            if file_info.file_size_bytes == 0:
                return OrchestratorResult(
                    status="SKIPPED",
                    correlation_id=context.correlation_id,
                    queue_message_id=context.queue_message_id,
                    api_name=context.api_name_str,
                    dataset_name=context.dataset_name,
                    layer_name=context.etl_layer.value,
                    env=context.env.value,
                    message=f"File with payload_hash {file_info.payload_hash} already exists. Upload skipped.",
                    output_path=None,  # brak nowego pliku
                    api_response_status_code=api_response_status_code)
            else:
                return OrchestratorResult(
                    status="COMPLETED",
                    correlation_id=context.correlation_id,
                    queue_message_id=context.queue_message_id,
                    api_name=context.api_name_str,
                    dataset_name=context.dataset_name,
                    layer_name=context.etl_layer.value, 
                    env=context.env,
                    message="API data successfully processed and stored to Bronze.",
                    output_path=final_output_path, # Używamy pobranej ścieżki
                    api_response_status_code=api_response_status_code # Używamy pobranego statusu
                )

        except Exception as e:
            logger.error(f"Error in BronzeOrchestrator for {context.api_name_str}: {e}")
            
            # Przygotowanie error_details
            error_details = {
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc()
            }
            
            # Zwróć OrchestratorResult w przypadku błędu
            return OrchestratorResult(
                status="FAILED",
                correlation_id=context.correlation_id,
                queue_message_id=context.queue_message_id,
                api_name=context.api_name_str,
                dataset_name=context.dataset_name,
                layer_name=context.etl_layer.value, 
                env=context.env,
                message=f"Bronze orchestration failed: {str(e)}",
                output_path=final_output_path, # Będzie None, jeśli błąd nastąpił przed uploadem, lub URL jeśli po
                api_response_status_code=api_response_status_code, # Używamy pobranego statusu
                error_details=error_details
            )