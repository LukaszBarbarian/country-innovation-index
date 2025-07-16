# src/ingestion/processors/json_data_processor.py
import json
from .base_data_processor import BaseDataProcessor
from ...common.models.ingestion_context import IngestionContext

class JsonDataProcessor(BaseDataProcessor):
    def process_and_save(self, context: IngestionContext) -> str:
        if not context.raw_api_response or context.api_response_status_code != 200:
            raise ValueError("Invalid API response in context for JSON processing.")
        
        # Zakładamy, że raw_api_response jest obiektem requests.Response
        data_to_save = context.raw_api_response.json()
        
        # Dynamiczne generowanie ścieżki (można rozbudować o template engine)
        # Przykładowy wzorzec: {api_name}/{dataset_name}/{year}/{month}/{day}/{timestamp}.json
        blob_path = self.storage_manager.save_data(
            data_to_save, 
            context.api_name, 
            context.dataset_name, 
            "json", 
            context.ingestion_timestamp,
            # Możesz dodać parametry ścieżki do IngestionContext
        )
        context.set_target_blob_path(blob_path) # Zapisz ścieżkę w kontekście
        return blob_path