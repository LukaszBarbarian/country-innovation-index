from abc import ABC
from typing import Any, Dict

from src.common.enums.domain_source import DomainSource
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer


class BaseParser(ABC):

    def __init__(self, correlation_id: str) -> None:
        self.correlation_id = correlation_id
    
    def _ensure_requires(self, requires: list[str], payload: Dict[str, Any]):
        if not all(field in payload for field in requires):
            missing_fields = [f for f in requires if f not in payload]
            raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

    def _map_etl_layer(self, etl_layer: str) -> ETLLayer:
        try:
            return ETLLayer(etl_layer)
        except ValueError:
            raise ValueError(f"Invalid ETL Layer: '{etl_layer}'")

    def _map_env(self, env_str: str) -> Env:
        try:
            return Env(env_str)
        except ValueError:
            raise ValueError(f"Invalid Env: '{env_str}'")

    def _map_domain_source(self, domain_source_str: str) -> DomainSource:
        try:
            return DomainSource(domain_source_str)
        except ValueError:
            raise ValueError(f"Invalid domain_source: '{domain_source_str}'")