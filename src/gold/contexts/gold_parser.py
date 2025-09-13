import datetime
import json
from typing import Optional
from dacite import Config, from_dict

from src.common.contexts.base_parser import BaseParser
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.model_type import ModelType
from src.common.enums.model_type_table import ModelTypeTable
from src.gold.contexts.gold_layer_context import GoldContext
from src.gold.models.models import GoldManifest, GoldSummary


class GoldParser(BaseParser):
    """
    A parser class responsible for converting JSON manifest and summary data
    into structured Python objects for the Gold ETL layer.

    This parser uses the `dacite` library to map raw dictionary data into
    dataclass instances, ensuring type safety and data integrity.
    """
    def parse(self, manifest_json: str, summary_json: Optional[str] = None) -> GoldContext:
        """
        Parses JSON strings for a manifest and an optional summary into a GoldContext object.

        Args:
            manifest_json (str): A JSON string representing the Gold layer manifest.
            summary_json (Optional[str]): An optional JSON string for the ETL run summary.

        Returns:
            GoldContext: A structured context object containing the parsed manifest and summary.
        """
        manifest_raw = json.loads(manifest_json)
        summary_raw = json.loads(summary_json) if summary_json else {}

        # ---- Manifest mapping ----
        manifest_env = Env(manifest_raw.get("env", "unknown"))

        def _map_models(items: list, model_type: ModelTypeTable):
            """Helper function to map a list of models from the manifest."""
            return [
                {
                    "name": item["name"],
                    "source_models": [ModelType(m) for m in item.get("source_models", [])],
                    "primary_keys": item.get("primary_keys", []),
                    "type": model_type,
                }
                for item in items
            ]

        models = []
        models.extend(_map_models(manifest_raw.get("dims", []), ModelTypeTable.DIM))
        models.extend(_map_models(manifest_raw.get("facts", []), ModelTypeTable.FACT))

        manifest: GoldManifest = from_dict(
            GoldManifest,
            {"models": models, "env": manifest_env, "etl_layer": ETLLayer.GOLD},
            config=Config(cast=[str, Env, ModelType, ETLLayer, ModelTypeTable]),
        )

        # ---- Summary mapping ----
        if summary_raw:
            summary_raw["env"] = Env(summary_raw.get("env", "unknown"))
            summary_raw["etl_layer"] = ETLLayer.GOLD
            for r in summary_raw.get("results", []):
                r["model"] = ModelType(r["model"])

            summary: GoldSummary = from_dict(
                GoldSummary,
                summary_raw,
                config=Config(
                    cast=[str, Env, ETLLayer, ModelType],
                    type_hooks={datetime.datetime: datetime.datetime.fromisoformat},
                ),
            )
            summary.correlation_id = summary_raw.get("correlation_id", "UNKNOWN")
        else:
            summary = GoldSummary(
                results=[],
                env=manifest.env,
                etl_layer=ETLLayer.GOLD,
                correlation_id="UNKNOWN",
            )

        # ---- Context ----
        return GoldContext(
            env=manifest.env,
            etl_layer=ETLLayer.GOLD,
            manifest=manifest,
            summary=summary,
            correlation_id=summary.correlation_id,
        )