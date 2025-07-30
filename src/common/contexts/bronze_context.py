# src/common/context/ingestion_context.py

from datetime import datetime
from typing import Dict, Any, Optional
import uuid
from src.common.enums.domain_source import DomainSource
from src.common.enums.etl_layers import ETLLayer
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.models.file_info import FileInfo

class BronzeContext(BaseLayerContext):
    """
    Context class specific to the Bronze/Ingestion layer.
    Extends BaseLayerContext with details relevant to API ingestion.
    """
    def __init__(self,
                 api_name_str: str,
                 dataset_name: str,
                 api_request_payload: Dict[str, Any],
                 raw_api_response: Optional[Any] = None,
                 api_response_status_code: Optional[int] = None,
                 ingestion_date_utc: Optional[datetime] = None,
                 correlation_id: Optional[str] = None,
                 file_info: Optional[FileInfo] = None
                ):
        
        mapped_domain_source = self._map_api_name_to_domain_source(api_name_str.upper())
        
        super().__init__(
            correlation_id=correlation_id,
            domain_source=mapped_domain_source,
            etl_layer=ETLLayer.BRONZE,
            ingestion_time_utc=ingestion_date_utc
        )

        self.api_name_str = api_name_str
        self.dataset_name = dataset_name
        self.api_request_payload = api_request_payload
        self.raw_api_response = raw_api_response
        self.api_response_status_code = api_response_status_code
        self.file_info = file_info

    def _map_api_name_to_domain_source(self, api_name_str: str) -> DomainSource:
        """
        Maps an API name string to a DomainSource enum member.
        Private method for internal use during initialization.
        """
        try:
            domain_source_member = DomainSource(api_name_str)
            return domain_source_member
        except ValueError:
            raise ValueError(f"Invalid API name: '{api_name_str}'. Expected one of: {[e.value for e in DomainSource]}")
        except Exception as e:
            raise RuntimeError(f"An unexpected error occurred during API name mapping: {e}")

    def set_api_response(self, response: Any, status_code: int):
        """
        Sets the raw API response and its status code in the context.
        """
        self.raw_api_response = response
        self.api_response_status_code = status_code

    @classmethod
    def from_queue_message(cls, message_body: Dict[str, Any]) -> 'BronzeContext':
        """
        Factory method to create an IngestionContext from a queue message.
        """
        try:
            api_name = message_body['api_name']
            correlation_id = message_body.get('correlation_id', str(uuid.uuid4()))
            
            return cls(
                api_name_str=api_name,
                dataset_name=message_body.get('dataset_name', api_name),
                api_request_payload=message_body.get('api_request_payload', {}),
                correlation_id=correlation_id
            )
        except KeyError as e:
            raise ValueError(f"Missing required field in queue message body: {e}. Message body: {message_body}")
        except Exception as e:
            raise RuntimeError(f"Error creating IngestionContext from queue message: {e}", exc_info=True)