# src/common/contexts/payload_parser.py

from src.common.contexts.layer_context import LayerContext
from typing import Dict, Any, Optional, cast
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env


class PayloadParser():
    def __init__(self):
        pass

    def parse(self, payload: Dict[str, Any]) -> LayerContext:
        self._ensure_requires(requires=["correlation_id", "queue_message_id", "etl_layer", "env"], payload=payload)

        source_config_payload = payload.get("source_config_payload")
        if not source_config_payload:
            raise ValueError("Payload must contain 'source_config_payload' key.")

        self._ensure_requires(
            requires=["domain_source_type", "domain_source", "dataset_name"],
            payload=source_config_payload
        )

        domain_source_type = DomainSourceType(source_config_payload["domain_source_type"])
        domain_source = self._map_domain_source(source_config_payload["domain_source"])
        dataset_name = source_config_payload["dataset_name"]
        correlation_id = cast(str, payload.get("correlation_id"))
        queue_message_id = cast(str, payload.get("queue_message_id"))
        etl_layer = self._map_etl_layer(cast(str, payload.get("etl_layer")))
        env = self._map_env(cast(str, payload.get("env")))
        request_payload = source_config_payload.get("request_payload", {})
        source_config_payload = source_config_payload
                
        return LayerContext(
            correlation_id=correlation_id,
            queue_message_id=queue_message_id,
            etl_layer=etl_layer,
            domain_source_type=domain_source_type,
            domain_source=domain_source,
            dataset_name=dataset_name,
            env=env,
            payload=payload,
            request_payload=request_payload,
            source_config_payload=source_config_payload
        )

    

    def _map_etl_layer(self, etl_layer: str) -> ETLLayer:
        try:
            return ETLLayer(etl_layer)
        except ValueError:
            raise ValueError(f"Invalid Layer name: '{etl_layer}'. Expected one of: {[e.value for e in ETLLayer]}")

    def _map_env(self, env_str: str) -> Env:
        try:
            return Env(env_str)
        except ValueError:
            raise ValueError(f"Invalid Env name: '{env_str}'. Expected one of: {[e.value for e in Env]}")

    def _ensure_requires(self, requires: list[str], payload: Dict[str, Any]):
        if not all(field in payload for field in requires):
            missing_fields = [f for f in requires if f not in payload]
            raise ValueError(f"Missing required fields in 'payload': {', '.join(missing_fields)}")

    def _map_domain_source(self, domain_source_str: str) -> DomainSource:
        try:
            return DomainSource(domain_source_str)
        except ValueError:
            valid_values = [e.value for e in DomainSource]
            raise ValueError(f"Invalid domain_source: '{domain_source_str}'. Expected one of: {valid_values}")