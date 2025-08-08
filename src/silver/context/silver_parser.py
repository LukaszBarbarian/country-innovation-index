# src/parsers/silver_payload_parser.py
from typing import Dict, Any, List, Optional, cast
from src.common.contexts.base_parser import BaseParser
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.silver.context.silver_context import SilverLayerContext

class SilverPayloadParser(BaseParser):
    def parse(self, payload: Dict[str, Any]) -> SilverLayerContext:
        self._ensure_requires(
            requires=[
                "status", "correlation_id", "queue_message_id",
                "domain_source", "domain_source_type", "dataset_name",
                "layer_name", "env", "output_paths"
            ],
            payload=payload
        )

        status = cast(str, payload["status"])
        correlation_id = cast(str, payload["correlation_id"])
        queue_message_id = cast(str, payload["queue_message_id"])
        domain_source = self._map_domain_source(cast(str, payload["domain_source"]))
        domain_source_type = DomainSourceType(cast(str, payload["domain_source_type"]))
        dataset_name = cast(str, payload["dataset_name"])
        etl_layer = self._map_etl_layer(cast(str, payload["layer_name"]))
        env = self._map_env(cast(str, payload["env"]))
        output_paths = cast(List[str], payload["output_paths"])
        message = cast(Optional[str], payload.get("message"))
        source_response_status_code = cast(Optional[int], payload.get("source_response_status_code"))
        error_details = cast(Optional[Dict[str, Any]], payload.get("error_details"))

        return SilverLayerContext(
            status=status,
            correlation_id=correlation_id,
            queue_message_id=queue_message_id,
            domain_source=domain_source,
            domain_source_type=domain_source_type,
            dataset_name=dataset_name,
            etl_layer=etl_layer,
            env=env,
            message=message,
            source_response_status_code=source_response_status_code,
            error_details=error_details,
            payload=payload,
            file_paths=output_paths
        )

