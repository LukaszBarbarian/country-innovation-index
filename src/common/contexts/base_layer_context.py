# src/common/contexts/base_layer_context.py

from dataclasses import dataclass, field
import datetime
from functools import cache
from typing import Dict, Any, List, Optional
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.utils.cache import Cache

@dataclass(frozen=True, kw_only=True)
class BaseLayerContext:
    # Fields without default values (required)
    correlation_id: str
    queue_message_id: str
    etl_layer: ETLLayer
    env: Env
    domain_source: DomainSource
    dataset_name: str
    domain_source_type: DomainSourceType
    file_paths: List[str]

    # Fields with default values (optional)
    payload: Dict[str, Any] = field(default_factory=dict)
    ingestion_time_utc: datetime.datetime = field(default_factory=datetime.datetime.utcnow)

    _cache: Cache = field(default_factory=Cache, repr=False, init=False)
