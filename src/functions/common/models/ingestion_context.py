# src/common/models/ingestion_context.py
from datetime import datetime
from typing import Dict, Any, Optional
from src.common.enums.file_format import FileFormat
from src.common.enums.domain_source import DomainSource

class IngestionContext:
    def __init__(self,
                 api_name: str,
                 dataset_name: str,
                 api_request_payload: Dict[str, Any],
                 raw_api_response: Optional[Any] = None,
                 api_response_status_code: Optional[int] = None,
                 file_format: Optional[FileFormat] = None,
                 target_blob_path: Optional[str] = None,
                 ingestion_timestamp: Optional[datetime] = None):
                 
        self.api_name = self.map_api_name_to_domain_source(api_name.upper())
        self.dataset_name = dataset_name
        self.api_request_payload = api_request_payload
        self.raw_api_response = raw_api_response
        self.api_response_status_code = api_response_status_code
        self.file_format = file_format
        self.target_blob_path = target_blob_path
        self.ingestion_timestamp = ingestion_timestamp if ingestion_timestamp else datetime.utcnow()

    def map_api_name_to_domain_source(self, api_name_str: str) -> DomainSource:
        try:    
            domain_source_member = DomainSource(api_name_str)
            return domain_source_member
        except ValueError:
            # Obsługa przypadku, gdy string nie pasuje do żadnego z członków enum
            # (czyli nie ma DomainSource.value == api_name_str)
            raise ValueError(f"Nieprawidłowa wartość api_name: '{api_name_str}'. Oczekiwano jednej z wartości: {[e.value for e in DomainSource]}")
        except Exception as e:
            # Inne nieoczekiwane błędy
            raise RuntimeError(f"Wystąpił nieoczekiwany błąd podczas mapowania api_name: {e}")


    def set_api_response(self, response: Any, status_code: int):
        self.raw_api_response = response
        self.api_response_status_code = status_code

    def set_file_format(self, file_format: FileFormat):
        self.file_format = file_format

    def set_target_blob_path(self, path: str):
        self.target_blob_path = path