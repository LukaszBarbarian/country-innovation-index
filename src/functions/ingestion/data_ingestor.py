# src/ingestion/data_ingestor.py
import logging
from ..common.models.ingestion_context import IngestionContext
from .api_clients.api_client_factory import ApiClientFactory
from .processors.base_data_processor import BaseDataProcessor
from .processors.json_data_processor import JsonDataProcessor # Importuj potrzebne procesory
from ..common.storage.bronze_storage_manager import BronzeStorageManager
from ..common.config_manager import ConfigManager # Importujemy ConfigManager

logger = logging.getLogger(__name__)

class DataIngestor:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.storage_manager = BronzeStorageManager() # Możesz wstrzykiwać StorageManager
        
    async def ingest(self, context: IngestionContext) -> str:
        logger.info(f"Starting ingestion for API: {context.api_name}, Dataset: {context.dataset_name}")

        # 1. Pobierz klienta API
        api_client = ApiClientFactory.get_client(context.api_name, self.config)
        context.set_file_format(api_client.default_file_format) # Ustaw domyślny format w kontekście

        # 2. Pobierz dane z API
        api_response = await api_client.fetch_data(**context.request_params)
        context.set_api_response(api_response, api_response.status_code) # Zapisz surową odpowiedź w kontekście

        if api_response.status_code != 200:
            raise Exception(f"API call failed with status {api_response.status_code}: {api_response.text()}")

        # 3. Wybierz odpowiedni procesor danych na podstawie formatu
        # Tutaj można użyć fabryki procesorów, jeśli masz ich wiele
        data_processor: BaseDataProcessor
        if context.file_format == "json":
            data_processor = JsonDataProcessor(self.storage_manager)
        # elif context.file_format == "csv":
        #    data_processor = CsvDataProcessor(self.storage_manager)
        else:
            raise ValueError(f"No data processor found for format: {context.file_format}")

        # 4. Przetwórz i zapisz dane
        blob_path = data_processor.process_and_save(context)
        logger.info(f"Data saved to {blob_path}")
        
        return blob_path