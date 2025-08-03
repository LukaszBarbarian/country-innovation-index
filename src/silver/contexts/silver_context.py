from datetime import datetime
from typing import Optional, Dict, Any
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.contexts.base_layer_context import BaseLayerContext


class SilverContext(BaseLayerContext):
    """
    Kontekst warstwy Silver – zawiera wspólne pola z BaseLayerContext oraz
    specyficzne dane z processing_config_payload.
    """
    def __init__(self,
                 correlation_id: str,
                 queue_message_id: str,
                 env: Env,
                 status: str,
                 api_name: str,
                 dataset_name: str,
                 layer_name: str,
                 message: str,
                 api_response_status_code: int,
                 output_path: str,
                 error_details: Optional[str] = None,
                 ingestion_time_utc: Optional[datetime] = None,
                 processing_config_payload: Optional[Dict[str, Any]] = None):

        super().__init__(
            correlation_id=correlation_id,
            queue_message_id=queue_message_id,
            etl_layer=ETLLayer.SILVER,
            ingestion_time_utc=ingestion_time_utc,
            processing_config_payload=processing_config_payload,
            env=env
        )

        self.status = status
        self.api_name = api_name
        self.dataset_name = dataset_name
        self.layer_name = layer_name
        self.message = message
        self.api_response_status_code = api_response_status_code
        self.output_path = output_path
        self.error_details = error_details
