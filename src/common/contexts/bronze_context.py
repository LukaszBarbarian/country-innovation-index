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
                 queue_message_id: str = None, # Upewnij się, że to może być None (Optional[str]) jeśli to przychodzi z ADF jako string
                 file_info: Optional[FileInfo] = None
                ):
        
        # Generowanie UUID dla correlation_id i queue_message_id, jeśli nie zostały podane
        # Te wartości będą nadpisywane, jeśli przyjdą z payloadu
        _correlation_id = correlation_id if correlation_id is not None else str(uuid.uuid4())
        _queue_message_id = queue_message_id if queue_message_id is not None else str(uuid.uuid4())

        mapped_domain_source = self._map_api_name_to_domain_source(api_name_str.upper())
        
        super().__init__(
            correlation_id=_correlation_id,
            queue_message_id=_queue_message_id,
            domain_source=mapped_domain_source,
            etl_layer=ETLLayer.BRONZE,
            ingestion_time_utc=ingestion_date_utc if ingestion_date_utc is not None else datetime.utcnow()
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

    def set_file_info(self, file_info: FileInfo):
        """
        Sets the FileInfo object in the context after file upload.
        """
        self.file_info = file_info

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]):
        """
        Tworzy instancję BronzeContext z ogólnego słownika payload (np. z ADF).
        Payload powinien mieć strukturę:
        {
          "correlation_id": "...",
          "queue_message_id": "...",
          "api_config_payload": {
            "api_name": "...",
            "dataset_name": "...",
            "api_request_payload": { ... }
          }
        }
        """
        # 1. Walidacja kluczy najwyższego poziomu
        if "api_config_payload" not in payload:
            raise ValueError("Payload must contain 'api_config_payload' key.")
        
        api_config_payload = payload["api_config_payload"]

        # 2. Walidacja kluczy w api_config_payload
        required_api_config_fields = ["api_name", "dataset_name"]
        if not all(field in api_config_payload for field in required_api_config_fields):
            missing_fields = [f for f in required_api_config_fields if f not in api_config_payload]
            raise ValueError(f"Missing required fields in 'api_config_payload': {', '.join(missing_fields)}")

        # 3. Wyciągnięcie wartości z payloadu
        correlation_id = payload.get("correlation_id")
        queue_message_id = payload.get("queue_message_id")
        
        # Jeśli correlation_id lub queue_message_id nie zostały podane w payloadzie
        # (co jest możliwe, jeśli ADF ich nie ustawia domyślnie, ale my to robimy)
        # to __init__ je wygeneruje.
        # Jeśli przyszły z ADF jako "null", to też zostaną potraktowane jako None.

        api_name_str = api_config_payload["api_name"]
        dataset_name = api_config_payload["dataset_name"]
        api_request_payload = api_config_payload.get("api_request_payload", {}) # Upewnij się, że to słownik

        # 4. Utworzenie i zwrócenie instancji BronzeContext
        return cls(
            api_name_str=api_name_str,
            dataset_name=dataset_name,
            api_request_payload=api_request_payload,
            correlation_id=correlation_id,
            queue_message_id=queue_message_id,
            ingestion_date_utc=datetime.utcnow() # Zawsze ustawiaj bieżącą datę UTC
        )