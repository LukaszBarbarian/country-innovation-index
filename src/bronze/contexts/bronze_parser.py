# src/parsers/bronze_payload_parser.py
from typing import Dict, Any, List, Optional, cast
from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.contexts.base_parser import BaseParser
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env

class BronzePayloadParser(BaseParser):
    def parse(self, payload: Dict[str, Any]) -> BronzeLayerContext:
        self._ensure_requires(requires=["correlation_id", "queue_message_id", "etl_layer", "env"], payload=payload)
        
        source_config_payload = payload.get("source_config_payload")
        if not source_config_payload:
            raise ValueError("Payload must contain 'source_config_payload' key.")

        self._ensure_requires(
            requires=["domain_source_type", "domain_source", "dataset_name"],
            payload=source_config_payload
        )

        correlation_id = cast(str, payload["correlation_id"])
        queue_message_id = cast(str, payload["queue_message_id"])
        etl_layer = self._map_etl_layer(cast(str, payload["etl_layer"]))
        env = self._map_env(cast(str, payload["env"]))
        
        domain_source_type = DomainSourceType(source_config_payload["domain_source_type"])
        domain_source = self._map_domain_source(source_config_payload["domain_source"])
        dataset_name = source_config_payload["dataset_name"]
        
        request_payload = source_config_payload.get("request_payload", {})
        
        file_path: Optional[List[str]] = None


        if domain_source_type == DomainSourceType.STATIC_FILE:
            file_path = cast(List[str], request_payload.get("file_path", []))


        return BronzeLayerContext(
            correlation_id=correlation_id,
            queue_message_id=queue_message_id,
            etl_layer=etl_layer,
            env=env,
            domain_source=domain_source,
            domain_source_type=domain_source_type,
            dataset_name=dataset_name,
            payload=payload,
            source_config_payload=source_config_payload,
            request_payload=request_payload,
            file_paths=file_path or []
        )

    