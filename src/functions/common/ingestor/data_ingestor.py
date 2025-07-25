# src/ingestion/data_ingestor.py
import logging
from src.functions.common.models.ingestion_context import IngestionContext
from src.functions.common.api_clients.api_client_factory import ApiClientFactory
from src.functions.common.processors.base_data_processor import BaseDataProcessor
from src.functions.common.processors.json_data_processor import JsonDataProcessor
from src.common.storage_account.bronze_storage_manager import BronzeStorageManager
from src.functions.common.config.config_manager import ConfigManager
from src.common.enums.file_format import FileFormat

logger = logging.getLogger(__name__)

class DataIngestor:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.storage_manager = BronzeStorageManager()
        
    async def ingest(self, context: IngestionContext) -> str:
        logger.info(f"Starting ingestion for Dataset: {context.dataset_name} using API identified as: {context.api_name}")

        api_client = ApiClientFactory.get_client(context.api_name, self.config)
        
        context.api_name = api_client.api_identifier 
        context.set_file_format(api_client.default_file_format)

        api_response = await api_client.fetch_data(context.api_request_payload) 
        context.set_api_response(api_response, api_response.status_code)

        if api_response.status_code != 200:
            raise Exception(f"API call failed with status {api_response.status_code}: {api_response.text()}")

        data_processor: BaseDataProcessor
        if context.file_format == FileFormat.JSON:
            data_processor = JsonDataProcessor(self.storage_manager)
        else:
            raise ValueError(f"No data processor found for format: {context.file_format}")

        blob_path = data_processor.process_and_save(context)
        logger.info(f"Data saved to {blob_path}")
        
        return blob_path