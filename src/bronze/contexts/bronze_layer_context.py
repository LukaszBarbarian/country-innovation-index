# src/common/contexts/bronze_layer_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from src.common.contexts.base_layer_context import BaseLayerContext

@dataclass(frozen=True, kw_only=True)
class BronzeLayerContext(BaseLayerContext):
    source_config_payload: Dict[str, Any] = field(default_factory=dict)
    request_payload: Dict[str, Any] = field(default_factory=dict)
    
    file_path: Optional[List[str]] = field(default_factory=list)
