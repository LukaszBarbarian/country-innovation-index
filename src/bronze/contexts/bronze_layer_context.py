# src/common/contexts/bronze_layer_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType

@dataclass(frozen=True, kw_only=True)
class BronzeLayerContext(BaseLayerContext):
    # Pola, które były w BaseLayerContext, ale zostały przeniesione,
    # ponieważ nie są uniwersalne dla wszystkich kontekstów (np. Silver z listą wyników)
    domain_source: DomainSource
    domain_source_type: DomainSourceType
    dataset_name: str
    file_paths: List[str] = field(default_factory=list)

    # Pola specyficzne dla warstwy Bronze
    source_config_payload: Dict[str, Any] = field(default_factory=dict)
    request_payload: Dict[str, Any] = field(default_factory=dict)