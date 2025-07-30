# src/ingestion/data_ingestor.py

import logging
from src.common.contexts.bronze_context import BronzeContext
from src.common.factories.api_client_factory import ApiClientFactory
from src.common.factories.data_processor_factory import DataProcessorFactory
from src.common.storage_account.bronze_storage_manager import BronzeStorageManager
from src.common.config.config_manager import ConfigManager
from src.common.storage_file_builder.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.models.file_info import FileInfo 
from src.common.models.processed_result import ProcessedResult 
from typing import List

logger = logging.getLogger(__name__)

class BronzeOrchestrator(BaseOrchestrator):
    def __init__(self, config):
        super().__init__(config)
         
        self.storage_manager = BronzeStorageManager()

    async def run(self, context: BronzeContext):
        logger.info(f"Starting ingestion for Dataset: {context.dataset_name} using API identified as: {context.domain_source} with CorrelationId: {context.correlation_id}")

        api_client = ApiClientFactory.get_instance(context.domain_source, self.config)
        data_processor = DataProcessorFactory.get_instance(context.domain_source)

        all_processed_records_results: List[ProcessedResult] = [] 

        try:
            async with api_client as client:
                logger.info(f"Requesting all records from {context.domain_source.value} via its generator...")
                async for raw_record in client.fetch_all_records(context.api_request_payload):
                    processed_record_result = data_processor.process(raw_record, context) 
                    all_processed_records_results.append(processed_record_result)
                    
            logger.info(f"Finished fetching all records for {context.dataset_name}. Total records fetched: {len(all_processed_records_results)}")

            if not all_processed_records_results:
                logger.info(f"No records fetched for {context.dataset_name} with CorrelationId: {context.correlation_id}. Skipping file save and Event Grid notification.")
                return

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
        except:
            raise
       