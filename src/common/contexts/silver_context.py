# src/silver/context/silver_context.py

from datetime import datetime
from typing import Optional, Dict, Any
import uuid
from src.common.enums.etl_layers import ETLLayer
from src.common.contexts.base_layer_context import BaseLayerContext

class SilverContext(BaseLayerContext):
    """
    Kontekst dla warstwy Silver.
    Dziedziczy metadane z BaseLayerContext i ustawia etl_layer na SILVER.
    """
    def __init__(self,
                 correlation_id: str,
                 queue_message_id: str,
                 ingestion_time_utc: Optional[datetime] = None,
                 processing_config_payload: Optional[Dict[str, Any]] = None): # Dodano nowy argument
        
        super().__init__(
            correlation_id=correlation_id,
            queue_message_id=queue_message_id,
            etl_layer=ETLLayer.SILVER,
            ingestion_time_utc=ingestion_time_utc,
            processing_config_payload=processing_config_payload # Przekazanie do bazowego kontekstu
        )


    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> 'SilverContext':
        """
        Tworzy instancję SilverContext z ogólnego słownika payload (np. z Azure Data Factory).
        Payload powinien zawierać 'correlation_id', 'queue_message_id',
        opcjonalnie 'ingestion_time_utc' oraz 'processing_config_payload'.
        """
        correlation_id = payload.get("correlation_id", str(uuid.uuid4()))
        queue_message_id = payload.get("queue_message_id", str(uuid.uuid4()))
        
        ingestion_time_utc = payload.get("ingestion_time_utc")
        if ingestion_time_utc and isinstance(ingestion_time_utc, str):
            try:
                ingestion_time_utc = datetime.fromisoformat(ingestion_time_utc.replace("Z", "+00:00"))
            except ValueError:
                pass

        processing_config_payload = payload.get("processing_config_payload", {}) # Pobranie payloadu

        return cls(
            correlation_id=correlation_id,
            queue_message_id=queue_message_id,
            ingestion_time_utc=ingestion_time_utc,
            processing_config_payload=processing_config_payload # Przekazanie payloadu
        )