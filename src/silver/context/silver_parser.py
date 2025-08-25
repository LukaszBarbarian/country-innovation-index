import os
from typing import Dict, List, Optional
from dacite import Config, MissingValueError, WrongTypeError, from_dict
from src.common.contexts.base_parser import BaseParser
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.model_type import ModelType
from src.common.models.ingestion_summary import IngestionSummary
from src.common.models.manifest import ManualDataPath, Model, SilverManifest
from src.silver.context.silver_context import SilverLayerContext


class SilverPayloadParser(BaseParser):
    def parse(self, manifest_payload: Dict, payload: Dict) -> SilverLayerContext:
        try:
            # Parsowanie manifestu
            silver_manifest = from_dict(
                data_class=SilverManifest,
                data=manifest_payload,
                config=Config(
                    type_hooks={
                        Env: Env,
                        ETLLayer: ETLLayer,
                        DomainSource: DomainSource,
                        ModelType: ModelType,
                    }
                )
            )

            # Parsowanie wyników ETL
            ingestion_summary = from_dict(
                data_class=IngestionSummary,
                data=payload,
                config=Config(
                    type_hooks={
                        Env: Env,
                        ETLLayer: ETLLayer,
                        DomainSource: DomainSource,
                        DomainSourceType: DomainSourceType,
                    }
                )
            )

            # Tworzenie ścieżek do storage
            storage_account_name = self._config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")
            base_path = f"https://{storage_account_name}.dfs.core.windows.net"

            for result in ingestion_summary.results:
                result.update_paths(f"{base_path}/{payload['etl_layer']}")

            # Aktualizacja ścieżek do tabel referencyjnych
            updated_references_tables = {
                key: base_path + value for key, value in silver_manifest.references_tables.items()
            }

            # Aktualizacja ścieżek do danych manualnych
            updated_manual_data_paths = [
                ManualDataPath(
                    domain_source=item.domain_source,
                    dataset_name=item.dataset_name,
                    file_path=base_path + item.file_path
                )
                for item in silver_manifest.manual_data_paths
            ]

            # Przekształcamy modele, nadając depends_on jako listę ModelType
            models_with_deps: List[Model] = []
            for m in silver_manifest.models:
                depends_on_list = [
                    dep if isinstance(dep, ModelType) else ModelType[dep]
                    for dep in getattr(m, "depends_on", []) or []
                ]
                models_with_deps.append(
                    Model(
                        model_name=m.model_name,
                        table_name=m.table_name,
                        source_datasets=m.source_datasets,
                        depends_on=depends_on_list
                    )
                )

            return SilverLayerContext(
                references_tables=updated_references_tables,
                manual_data_paths=updated_manual_data_paths,
                models=models_with_deps,
                ingestion_summary=ingestion_summary,
                etl_layer=silver_manifest.etl_layer,
                env=silver_manifest.env,
                correlation_id=payload['correlation_id'],
                is_valid=self._validate_context(ingestion_summary)
            )

        except (MissingValueError, WrongTypeError, KeyError) as e:
            raise ValueError(f"Błąd danych lub typowania w manifeście: {e}")
        except Exception as e:
            raise ValueError(f"Ogólny błąd tworzenia kontekstu: {e}")

    def _validate_context(self, summary: IngestionSummary) -> bool:
        """
        Waliduje kontekst na podstawie statusów w IngestionSummary.
        Zwraca True, jeśli status podsumowania to "COMPLETED" lub "PARTIAL_SUCCESS",
        oraz jeśli żaden z wyników nie ma statusu "FAILED".
        """
        if summary.status not in ["COMPLETED", "PARTIAL_SUCCESS"]:
            return False
        if any(result.status == "FAILED" for result in summary.results):
            return False
        return True
