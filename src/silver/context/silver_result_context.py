# src/common/contexts/silver_result_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env

@dataclass(frozen=True, kw_only=True)
class SilverResultContext:
    status: str
    correlation_id: str
    domain_source: DomainSource
    domain_source_type: DomainSourceType
    dataset_name: str
    etl_layer: ETLLayer
    env: Env
    
    message: Optional[str] = None
    source_response_status_code: Optional[int] = None
    error_details: Optional[Dict[str, Any]] = None
    output_paths: List[str] = field(default_factory=list)
    is_valid: bool = False