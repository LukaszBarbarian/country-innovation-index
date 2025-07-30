# src/common/context/base_layer_context.py

from datetime import datetime
from typing import Optional, Any
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.domain_source import DomainSource

class BaseLayerContext:
    """
    Base context class for all data processing layers (Bronze, Silver, Gold).
    Contains common metadata relevant across different stages of data processing.
    """
    def __init__(self,
                 correlation_id: str,
                 queue_message_id: str,
                 domain_source: DomainSource,
                 etl_layer: ETLLayer,
                 ingestion_time_utc: Optional[datetime] = None):
        
        self.correlation_id = correlation_id
        self.queue_message_id = queue_message_id
        self.domain_source = domain_source
        self.etl_layer = etl_layer
        self.ingestion_time_utc = ingestion_time_utc if ingestion_time_utc is not None else datetime.utcnow()