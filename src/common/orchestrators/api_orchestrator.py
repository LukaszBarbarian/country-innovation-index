from logging import config
import traceback
from typing import Any, List, Optional
from venv import logger
from src.common.config.config_manager import ConfigManager
from src.common.contexts.layer_context import LayerContext
from src.common.enums.domain_source_type import DomainSourceType
from src.common.factories.api_client_factory import ApiClientFactory
from src.common.factories.data_processor_factory import DataProcessorFactory
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.models.file_info import FileInfo
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.models.processed_result import ProcessedResult
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry
from src.common.storage_account.blob_storage_manager import BlobStorageManager

@OrchestratorRegistry.register(DomainSourceType.API)
class ApiOrchestrator(BaseOrchestrator):
    def __init__(self, config: ConfigManager):
        super().__init__(config)


    async def run(self, context: LayerContext) -> OrchestratorResult:
        storage_manager = BlobStorageManager(context.etl_layer.value)
        all_output_paths: List[str] = []
        source_response_status_code: Optional[int] = None
        all_processed_records_results: List[ProcessedResult] = []

        api_client = ApiClientFactory.get_instance(context.domain_source, config=self.config)

        try:
            async with api_client as client:
                results = await client.fetch_all(context)

                data_processor = DataProcessorFactory.get_instance(context.domain_source)
                for result in results:
                    for raw_record in result.data:
                        processed_record_result = data_processor.process(raw_record, context)
                        all_processed_records_results.append(processed_record_result)

            logger.info(
                f"Finished fetching all records for {context.dataset_name}. Total records fetched: {len(all_processed_records_results)}"
            )

            if not all_processed_records_results:
                logger.info(
                    f"No records fetched for {context.dataset_name} with CorrelationId: {context.correlation_id}. Skipping file save and Event Grid notification."
                )                

                return self.create_result(context=context, status="COMPLETED", 
                                          message="No records fetched, skipping file save.",
                                          all_output_paths=all_output_paths,
                                          source_response_status_code=context.source_response_status_code)
                
            file_metadata_builder = StorageFileBuilderFactory.get_instance(context.domain_source)

            for processed_result in all_processed_records_results:
                file_output = file_metadata_builder.build_file_output(
                    processed_records_results=processed_result,
                    context=context,
                    storage_account_name=self.storage_account_name,
                    container_name=storage_manager.container_name
                )
                file_content_bytes = file_output["file_content_bytes"]
                file_info: FileInfo = file_output["file_info"]

                file_info.file_size_bytes = await storage_manager.upload_blob(
                    file_content_bytes=file_content_bytes,
                    file_info=file_info)

                source_response_status_code = context.source_response_status_code
                all_output_paths.append(file_info.full_path_in_container)

            # Po zakończeniu uploadu sprawdzamy, czy mamy jakieś nowe pliki
            if not all_output_paths:
                return self.create_result(context=context, status="SKIPPED", message="No new files uploaded, all uploads skipped.", source_response_status_code=source_response_status_code)
            else:
                return self.create_result(context=context, status="COMPLETED", 
                                          message=f"API data successfully processed and stored to Bronze. Uploaded {len(all_output_paths)} files.",
                                          all_output_paths=all_output_paths,
                                          source_response_status_code=source_response_status_code)
                

        except Exception as e:
            logger.error(f"Error in BronzeOrchestrator for {context.domain_source.value}: {e}")

            error_details = {
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc()
            }

            return self.create_result(context=context, status="FAILED", message=f"Bronze orchestration failed: {str(e)}", error_details=error_details)
            
