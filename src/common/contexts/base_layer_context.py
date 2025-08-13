# src/common/contexts/base_layer_context.py

from dataclasses import dataclass, field
import datetime
from functools import cache
from typing import Dict, Any, List, Optional
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.utils.cache import Cache

@dataclass(frozen=True)
class BaseLayerContext:
    # Fields bez default values (wymagane)
    correlation_id: str
    etl_layer: ETLLayer
    env: Env

    # Fields z default values (opcjonalne)
    payload: Dict[str, Any] = field(default_factory=dict)
    ingestion_time_utc: datetime.datetime = field(default_factory=datetime.datetime.utcnow)
    _cache: Cache = field(default_factory=Cache, repr=False, init=False)
    is_valid: bool = False