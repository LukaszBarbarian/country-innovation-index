# src/parsers/silver_payload_parser.py
from typing import Dict, Any, List, Optional, cast

# Importujemy wszystkie niezbędne klasy
from src.common.contexts.base_parser import BaseParser
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.silver.context.silver_context import SilverLayerContext
from src.silver.context.silver_result_context import SilverResultContext

class SilverPayloadParser(BaseParser):
    def parse(self, payload: Dict[str, Any]) -> SilverLayerContext:
        """
        Parsuje cały payload na jeden obiekt SilverLayerContext
        zawierający listę zagnieżdżonych obiektów SilverResultContext.
        """
        
        self._ensure_requires(
            requires=["status", "correlation_id", "env"],
            payload=payload
        )

        status = cast(str, payload["status"])
        correlation_id = cast(str, payload["correlation_id"])
        etl_layer = ETLLayer.SILVER
        env = self._map_env(cast(str, payload["env"]))
        
        message = cast(Optional[str], payload.get("message"))
        source_response_status_code = cast(Optional[int], payload.get("source_response_status_code"))
        error_details = cast(Optional[Dict[str, Any]], payload.get("error_details"))

        results_list = cast(List[Dict[str, Any]], payload.get("results", []))
        silver_results: List[SilverResultContext] = []

        is_main_status_valid = status.endswith("_COMPLETED")

        for item in results_list:
            self._ensure_requires(
                requires=[
                    "status", "correlation_id", "domain_source",
                    "domain_source_type", "dataset_name", "layer_name", "env"
                ],
                payload=item
            )
            
            item_status = cast(str, item["status"])
            item_correlation_id = cast(str, item["correlation_id"])
            item_domain_source = self._map_domain_source(cast(str, item["domain_source"]))
            item_domain_source_type = DomainSourceType(cast(str, item["domain_source_type"]))
            item_dataset_name = cast(str, item["dataset_name"])
            item_etl_layer = self._map_etl_layer(cast(str, item["layer_name"]))
            item_env = self._map_env(cast(str, item["env"]))
            
            item_output_paths = cast(List[str], item.get("output_paths", []))
            item_message = cast(Optional[str], item.get("message"))
            item_source_response_status_code = cast(Optional[int], item.get("source_response_status_code"))
            item_error_details = cast(Optional[Dict[str, Any]], item.get("error_details"))

            # Sprawdzamy status dla pojedynczego wyniku
            is_result_valid = item_status == "COMPLETED"

            silver_results.append(
                SilverResultContext(
                    status=item_status,
                    correlation_id=item_correlation_id,
                    domain_source=item_domain_source,
                    domain_source_type=item_domain_source_type,
                    dataset_name=item_dataset_name,
                    etl_layer=item_etl_layer,
                    env=item_env,
                    message=item_message,
                    source_response_status_code=item_source_response_status_code,
                    error_details=item_error_details,
                    output_paths=item_output_paths,
                    is_valid=is_result_valid  # Ustawiamy flagę
                )
            )

                
        return SilverLayerContext(
            status=status,
            correlation_id=correlation_id,
            etl_layer=etl_layer,
            env=env,
            results=silver_results,
            message=message,
            source_response_status_code=source_response_status_code,
            error_details=error_details,
            is_valid=is_main_status_valid
        )