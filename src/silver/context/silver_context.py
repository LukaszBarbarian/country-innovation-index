# src/common/contexts/silver_layer_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType

@dataclass(frozen=True, kw_only=True)
class SilverLayerContext(BaseLayerContext):
    status: str
    domain_source: DomainSource
    domain_source_type: DomainSourceType
    dataset_name: str
    
    message: Optional[str] = None
    source_response_status_code: Optional[int] = None
    error_details: Optional[Dict[str, Any]] = None