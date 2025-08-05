# src/common/context/base_layer_context.py

from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from datetime import datetime
from typing import Optional, Any, Dict
from src.common.utils.cache import Cache

class BaseLayerContext:
    """
    Base context class for all data processing layers (Bronze, Silver, Gold).
    Contains common metadata relevant across different stages of data processing.
    """
    def __init__(self,
                 correlation_id: str,
                 queue_message_id: str,
                 etl_layer: ETLLayer,
                 env: Env,
                 ingestion_time_utc: Optional[datetime] = None,
                 processing_config_payload: Optional[Dict[str, Any]] = None):
        
        self.correlation_id = correlation_id
        self.queue_message_id = queue_message_id
        self.etl_layer = etl_layer
        self.env = env
        self.ingestion_time_utc = ingestion_time_utc if ingestion_time_utc is not None else datetime.utcnow()
        self.processing_config_payload = processing_config_payload if processing_config_payload is not None else {}
        self._cache: Cache = Cache()



    @property
    def cache(self) -> Cache:
        return self._cache


    @property
    def empty(self) -> str:
        return "N/A"