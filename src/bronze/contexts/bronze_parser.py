# src/bronze/parsers/bronze_payload_parser.py
import uuid
from typing import Dict, Any, List

from dacite import from_dict

from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.contexts.base_parser import BaseParser
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.models.ingestions import IngestionContext
from src.common.models.manifest import (
    SourceConfigPayload,
    PipelineConfig,   # model nadrzędny
)


class BronzePayloadParser(BaseParser):
    def parse(self, manifest_payload: Dict[str, Any]) -> BronzeLayerContext:
        """
        Parsuje manifest JSON -> BronzeLayerContext.
        request_payload jest przekazywany wprost (dict/RequestConfig) bez rozgałęzień po typach.
        """
        try:
            # JSON -> PipelineConfig (dataclass)
            pipeline_config = from_dict(data_class=PipelineConfig, data=manifest_payload)

            correlation_id = manifest_payload.get("correlation_id", str(uuid.uuid4()))
            env = Env(pipeline_config.env)
            etl_layer = ETLLayer(pipeline_config.etl_layer)

            ingest_contexts: List[IngestionContext] = []

            for ds in pipeline_config.sources:
                scp = ds.source_config_payload

                source_config = SourceConfigPayload(
                    domain_source=DomainSource(scp.domain_source),
                    domain_source_type=DomainSourceType(scp.domain_source_type),
                    dataset_name=scp.dataset_name,
                    request_payload=scp.request_payload,  # passthrough
                )

                ingest_contexts.append(
                    IngestionContext(
                        correlation_id=correlation_id,
                        etl_layer=etl_layer,
                        env=env,
                        source_config=source_config,
                    )
                )

            return BronzeLayerContext(
                correlation_id=correlation_id,
                etl_layer=etl_layer,
                env=env,
                ingest_contexts=ingest_contexts,
            )

        except KeyError as e:
            raise ValueError(f"Błąd parsowania manifestu: brak klucza {e}") from e
        except ValueError as e:
            raise ValueError(f"Nieprawidłowa wartość w manifeście: {e}") from e
