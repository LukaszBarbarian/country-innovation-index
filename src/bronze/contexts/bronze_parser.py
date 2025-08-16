# src/bronze/parsers/bronze_payload_parser.py - poprawiony
import uuid
from typing import Dict, Any, List
from dataclasses import asdict

from src.bronze.manifest.bronze_manifest import BronzeManifest
from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.contexts.base_parser import BaseParser
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.models.etl_model import EtlModel
from src.common.models.ingestions import IngestionContext
from src.common.models.manifest import ApiRequestPayload, StaticFileRequestPayload, SourceConfigPayload

class BronzePayloadParser(BaseParser):
    def parse(self, manifest_payload: Dict[str, Any]) -> BronzeLayerContext:
        """Parsuje cały manifest JSON i tworzy nadrzędny obiekt kontekstu."""
        try:
            correlation_id = manifest_payload.get("correlation_id", str(uuid.uuid4()))
            env_str = manifest_payload['env']
            etl_layer_str = manifest_payload['etl_layer']
            
            ingest_contexts: List[IngestionContext] = []
            for item in manifest_payload.get("sources", []):
                source_item = item['source_config_payload']
                domain_source_str = source_item['domain_source']
                domain_source_type_str = source_item['domain_source_type']
                dataset_name = source_item['dataset_name']
                request_payload_dict = source_item['request_payload']
                
                # Dynamiczne parsowanie payloadu
                if domain_source_type_str == "api":
                    parsed_request_payload = ApiRequestPayload(**request_payload_dict)
                elif domain_source_type_str == "static_file":
                    parsed_request_payload = StaticFileRequestPayload(**request_payload_dict)
                else:
                    raise ValueError(f"Nieznany typ źródła: {domain_source_type_str}")

                source_config = SourceConfigPayload(
                    env=Env(env_str),
                    etl_layer=ETLLayer(etl_layer_str),
                    domain_source=DomainSource(domain_source_str),
                    domain_source_type=DomainSourceType(domain_source_type_str),
                    dataset_name=dataset_name,
                    request_payload=parsed_request_payload
                )
                
                # Tworzymy pojedynczy kontekst pozyskiwania i dodajemy do listy
                ingest_contexts.append(IngestionContext(
                    correlation_id=correlation_id,
                    etl_layer=ETLLayer(etl_layer_str),
                    env=Env(env_str),
                    source_config=source_config
                ))
            
            # Zwracamy nadrzędny kontekst z listą kontekstów do pozyskania
            return BronzeLayerContext(
                correlation_id=correlation_id,
                etl_layer=ETLLayer(etl_layer_str),
                env=Env(env_str),
                ingest_contexts=ingest_contexts
            )

        except KeyError as e:
            raise ValueError(f"Błąd parsowania manifestu: brak klucza {e}")
        except ValueError as e:
            raise ValueError(f"Nieprawidłowa wartość w manifestu: {e}")