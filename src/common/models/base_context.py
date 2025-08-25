# src/common/contexts/base_layer_context.py

from abc import abstractmethod
from dataclasses import dataclass, field
import datetime
from typing import Dict, Any, List, Optional
from src.common.enums.domain_source import DomainSource
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.models.ingestion_result import IngestionResult
from src.common.utils.cache import Cache

@dataclass(frozen=True)
class BaseContext:
    correlation_id: str
    etl_layer: ETLLayer
    env: Env

    ingestion_time_utc: datetime.datetime = field(default_factory=datetime.datetime.utcnow)
    _cache: Cache = field(default_factory=Cache, repr=False, init=False)
    is_valid: bool = False



    @abstractmethod
    def get_ingestion_result(self, domain_source: DomainSource) -> Optional[List[IngestionResult]]:
        pass