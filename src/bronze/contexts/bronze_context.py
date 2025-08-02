# src/common/context/ingestion_context.py

from datetime import datetime
from typing import Dict, Any, Optional
import uuid
from src.common.enums.domain_source import DomainSource
from src.common.enums.etl_layers import ETLLayer
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.models.file_info import FileInfo
from src.common.enums.env import Env

class BronzeContext(BaseLayerContext):
    """
    Context class specific to the Bronze/Ingestion layer.
    Extends BaseLayerContext with details relevant to API ingestion.
    """
    def __init__(self,
                 env: Env,
                 api_name_str: str,
                 dataset_name: str,
                 api_request_payload: Dict[str, Any], # To jest specyficzne dla API
                 raw_api_response: Optional[Any] = None,
                 api_response_status_code: Optional[int] = None,
                 ingestion_date_utc: Optional[datetime] = None,
                 correlation_id: Optional[str] = None,
                 queue_message_id: Optional[str] = None,
                 file_info: Optional[FileInfo] = None,
                 api_config_payload: Optional[Dict[str, Any]] = None 
                 ):
        
        _correlation_id = correlation_id if correlation_id is not None else str(uuid.uuid4())
        _queue_message_id = queue_message_id if queue_message_id is not None else str(uuid.uuid4())

        super().__init__(
            correlation_id=_correlation_id,
            queue_message_id=_queue_message_id,
            etl_layer=ETLLayer.BRONZE,
            ingestion_time_utc=ingestion_date_utc if ingestion_date_utc is not None else datetime.utcnow(),
            processing_config_payload=api_config_payload,
            env=env
        )

        self.api_name_str = api_name_str
        self.dataset_name = dataset_name
        self.api_request_payload = api_request_payload 
        self.raw_api_response = raw_api_response
        self.api_response_status_code = api_response_status_code
        self.file_info = file_info
        self.domain_source: DomainSource = None


    
    def set_api_response(self, response: Any, status_code: int):
        """
        Sets the raw API response and its status code in the context.
        """
        self.raw_api_response = response
        self.api_response_status_code = status_code

    def set_file_info(self, file_info: FileInfo):
        """
        Sets the FileInfo object in the context after file upload.
        """
        self.file_info = file_info

    # @classmethod
    # def from_payload(cls, payload: Dict[str, Any]):
    #     """
    #     Tworzy instancję BronzeContext z ogólnego słownika payload (np. z ADF).
    #     Payload powinien mieć strukturę:
    #     {
    #       "correlation_id": "...",
    #       "queue_message_id": "...",
    #       "api_config_payload": {
    #         "api_name": "...",
    #         "dataset_name": "...",
    #         "api_request_payload": { ... }
    #       }
    #     }
    #     """
    #     if "api_config_payload" not in payload:
    #         raise ValueError("Payload must contain 'api_config_payload' key.")
        
    #     api_config_payload_from_input = payload["api_config_payload"] # Zmieniono nazwę zmiennej

    #     required_api_config_fields = ["api_name", "dataset_name"]
    #     if not all(field in api_config_payload_from_input for field in required_api_config_fields):
    #         missing_fields = [f for f in required_api_config_fields if f not in api_config_payload_from_input]
    #         raise ValueError(f"Missing required fields in 'api_config_payload': {', '.join(missing_fields)}")

    #     correlation_id = payload.get("correlation_id")
    #     queue_message_id = payload.get("queue_message_id")
        
    #     api_name_str = api_config_payload_from_input["api_name"]
    #     dataset_name = api_config_payload_from_input["dataset_name"]
    #     api_request_payload = api_config_payload_from_input.get("api_request_payload", {})

    #     return cls(
    #         api_name_str=api_name_str,
    #         dataset_name=dataset_name,
    #         api_request_payload=api_request_payload,
    #         correlation_id=correlation_id,
    #         queue_message_id=queue_message_id,
    #         api_config_payload=api_config_payload_from_input, # Przekazujemy cały api_config_payload jako processing_config_payload
    #         ingestion_date_utc=datetime.utcnow()
    #     )