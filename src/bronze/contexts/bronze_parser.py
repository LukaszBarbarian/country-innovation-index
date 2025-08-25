# src/bronze/parsers/bronze_payload_parser.py
import uuid
from typing import Dict, Any, List

from dacite import Config, from_dict

from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.contexts.base_parser import BaseParser
from src.common.models.manifest import PipelineConfig
from src.common.models.ingestion_context import IngestionContext
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType


class BronzePayloadParser(BaseParser):
    def parse(self, manifest_payload: Dict[str, Any]) -> BronzeLayerContext:
        """
        Parsuje manifest JSON do BronzeLayerContext.
        Używa dacite do automatycznego mapowania zagnieżdżonych struktur.
        """
        try:
            # 1. Parsowanie całego manifestu za pomocą dacite
            pipeline_config = from_dict(
                data_class=PipelineConfig, 
                data=manifest_payload, 
                config=Config(
                    type_hooks={
                        Env: Env,
                        ETLLayer: ETLLayer,
                        DomainSource: DomainSource,
                        DomainSourceType: DomainSourceType,
                    }
                )
            )

            # Pobranie correlation_id z payloadu lub wygenerowanie nowego
            correlation_id = manifest_payload.get("correlation_id", str(uuid.uuid4()))

            ingest_contexts: List[IngestionContext] = []

            # 2. Iteracja po sparsowanych obiektach, a nie słownikach
            for ds in pipeline_config.sources:
                # Obiekt SourceConfigPayload został już utworzony przez dacite
                scp = ds.source_config_payload

                ingest_contexts.append(
                    IngestionContext(
                        correlation_id=correlation_id,
                        etl_layer=pipeline_config.etl_layer, # Używamy obiektu zmapowanego przez dacite
                        env=pipeline_config.env, # Używamy obiektu zmapowanego przez dacite
                        source_config=scp, # Cały obiekt, nie ręcznie tworzony
                    )
                )

            return BronzeLayerContext(
                correlation_id=correlation_id,
                etl_layer=pipeline_config.etl_layer,
                env=pipeline_config.env,
                ingest_contexts=ingest_contexts
            )

        except KeyError as e:
            raise ValueError(f"Błąd parsowania manifestu: brak klucza {e}") from e
        except ValueError as e:
            raise ValueError(f"Nieprawidłowa wartość w manifeście: {e}") from e
        except Exception as e:
            # Warto dodać ogólne przechwytywanie, np. dla błędów dacite
            raise ValueError(f"Ogólny błąd parsowania manifestu: {e}") from e