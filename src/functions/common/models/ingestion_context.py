# src/common/models/ingestion_context.py
from datetime import datetime
from typing import Dict, Any, Optional

class IngestionContext:
    def __init__(self,
                 api_name: str,
                 dataset_name: str,
                 request_params: Dict[str, Any],
                 raw_api_response: Optional[Any] = None, # Surowa odpowiedź z API (np. requests.Response)
                 api_response_status_code: Optional[int] = None,
                 file_format: Optional[str] = None, # np. 'json', 'csv', 'xml'
                 target_blob_path: Optional[str] = None,
                 ingestion_timestamp: Optional[datetime] = None):
        self.api_name = api_name
        self.dataset_name = dataset_name
        self.request_params = request_params
        self.raw_api_response = raw_api_response
        self.api_response_status_code = api_response_status_code
        self.file_format = file_format
        self.target_blob_path = target_blob_path
        self.ingestion_timestamp = ingestion_timestamp if ingestion_timestamp else datetime.utcnow()

    # Metody do aktualizacji kontekstu w trakcie procesu
    def set_api_response(self, response: Any, status_code: int):
        self.raw_api_response = response
        self.api_response_status_code = status_code

    def set_file_format(self, file_format: str):
        self.file_format = file_format

    def set_target_blob_path(self, path: str):
        self.target_blob_path = path