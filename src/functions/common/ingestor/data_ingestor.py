# src/ingestion/data_ingestor.py
import logging
from src.functions.common.models.ingestion_context import IngestionContext
from src.functions.common.api_clients.api_client_factory import ApiClientFactory
from src.functions.common.processors.data_processor_factory import DataProcessorFactory
from src.common.storage_account.bronze_storage_manager import BronzeStorageManager
from src.functions.common.config.config_manager import ConfigManager
from src.common.storage_metadata_file_builder.storage_metadata_file_builder_factory import StorageMetadataFileBuilderFactory
from datetime import datetime
from src.common.models.file_info import FileInfo
from typing import List, Any
from src.functions.common.event_grid.event_grid_publisher import EventGridPublisher # Ścieżka do publishera

logger = logging.getLogger(__name__)

class DataIngestor:
    """
    Klasa odpowiedzialna za orkiestrację procesu pobierania danych z API,
    ich przetwarzania i zapisu do warstwy Bronze w Data Lake.
    """
    def __init__(self, config: ConfigManager, event_publisher: EventGridPublisher):
        self.config = config
        self.event_publisher = event_publisher
        self.storage_manager = BronzeStorageManager()
        self.storage_account_name = self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")
        self.container_name = BronzeStorageManager.DEFAULT_CONTAINER_NAME

    async def ingest(self, context: IngestionContext):
        logger.info(f"Starting ingestion for Dataset: {context.dataset_name} using API identified as: {context.domain_source}")

        api_client = ApiClientFactory.get_instance(context.domain_source, self.config)

        all_processed_data: List[Any] = []
        data_processor = DataProcessorFactory.get_instance(context.domain_source)

        async with api_client as client:
            logger.info(f"Requesting all records from {context.domain_source.value} via its generator...")
            async for raw_record in client.fetch_all_records(context.api_request_payload):
                processed_record = data_processor.process(raw_record)
                all_processed_data.append(processed_record)
                
        logger.info(f"Finished fetching all records for {context.dataset_name}. Total records fetched: {len(all_processed_data)}")

        # Jeśli nie ma danych do zapisania, nie ma sensu dalej iść i wysyłać Event Grid
        if not all_processed_data:
            logger.info(f"No records fetched for {context.dataset_name}. Skipping file save and Event Grid notification.")
            return

        # Budowanie ścieżki zapisu w Data Lake (do warstwy Bronze)
        ingestion_date = datetime.now().strftime("%Y-%m-%d") 
        
        path_kwargs = {}
        if context.api_request_payload:
            # Dodaj parametry payloadu do ścieżki, jeśli są istotne dla partycjonowania danych
            path_kwargs.update(context.api_request_payload) 
        
        
        file_info: FileInfo = StorageMetadataFileBuilderFactory.get_instance(context.domain_source).build_file_info(
            domain_source=context.domain_source,
            dataset_name=context.dataset_name,
            ingestion_date=ingestion_date,
            data_lake_storage_account_name=self.storage_account_name,
            container_name=self.container_name,
            **context.api_request_payload
        )
        
        
        file_size_bytes = self.storage_manager.upload_processed_result(result=all_processed_data, blob_path=file_info.full_path_in_container)
        file_info.file_size_bytes = file_size_bytes

        
        logger.info(f"Data successfully saved to blob path: {file_info.full_path_in_container}. Final record count: {len(all_processed_data)}, File size: {file_size_bytes} bytes")

        try:
            if self.event_publisher and file_info.file_size_bytes is not None:
                publish_success = self.event_publisher.publish_file_upload_event(
                    file_info=file_info, 
                        correlation_id=str(context.api_request_payload["correlationId"]) if "correlationId" in context.api_request_payload else None
                )
                if publish_success:
                    logger.info(f"Event Grid notification sent for file: {file_info.file_name}")
                else:
                    logger.warning(f"Failed to send Event Grid notification for file: {file_info.file_name}.")
            elif not self.event_publisher:
                logger.warning("Event Grid publisher not provided, skipping event notification.")
            else:
                logger.warning(f"File size not available for {file_info.file_name}, skipping Event Grid notification.")

        except Exception as e:
            logger.error(f"Error while publishing Event Grid event for {file_info.file_name}: {e}", exc_info=True) # Dodano exc_info=True dla pełnego stack trace
