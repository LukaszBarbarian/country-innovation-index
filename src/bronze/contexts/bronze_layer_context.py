# src/common/contexts/bronze_layer_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from src.common.models.base_context import BaseContext
from src.common.models.ingestion_context import IngestionContext

@dataclass(frozen=True, kw_only=True)
class BronzeLayerContext(BaseContext):   
    ingest_contexts: List[IngestionContext] = field(default_factory=list)
