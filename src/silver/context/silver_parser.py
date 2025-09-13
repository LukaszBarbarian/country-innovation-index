import datetime
import json
import os
from dacite import Config, from_dict
from typing import Optional, Dict, List
from src.common.contexts.base_parser import BaseParser
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.model_type import ModelType
from src.common.enums.reference_source import ReferenceSource
from src.silver.context.silver_context import SilverContext
from src.silver.models.models import SilverManifest, SilverSummary


class SilverParser(BaseParser):
    """
    Parses manifest and summary JSON strings into structured SilverContext objects.

    This class handles the conversion of raw JSON data, typically retrieved from
    Azure Blob Storage, into Python dataclasses. It performs type mapping for
    various enums and handles nested data structures to correctly
    instantiate the `SilverContext` with its manifest and summary.
    """
    def parse(self, manifest_json: str, summary_json: Optional[str] = None) -> SilverContext:
        """
        Parses JSON strings for a manifest and an optional summary into a SilverContext.

        Args:
            manifest_json (str): The JSON string for the manifest.
            summary_json (Optional[str]): The JSON string for the summary.

        Returns:
            SilverContext: The fully populated context object for the Silver layer.
        """
        manifest_raw = json.loads(manifest_json)
        summary_raw = json.loads(summary_json)

        # Map env to enum
        manifest_raw['env'] = Env(manifest_raw.get('env', 'unknown'))

        if "references_tables" in manifest_raw:
            converted_refs = {
                ReferenceSource(key): value
                for key, value in manifest_raw["references_tables"].items()
            }
            manifest_raw["references_tables"] = converted_refs

        if "manual_data_paths" in manifest_raw:
            manifest_raw["manual_data_paths"] = [
                {
                    "domain_source": DomainSource(item["domain_source"]),
                    "dataset_name": item["dataset_name"],
                    "file_path": item["file_path"]
                }
                for item in manifest_raw["manual_data_paths"]
            ]

        # Source datasets enum mapping
        for model in manifest_raw.get("models", []):
            model["model_name"] = ModelType(model["model_name"])
            for ds in model.get("source_datasets", []):
                ds["domain_source"] = DomainSource(ds.get("domain_source", "UNKNOWN"))

        manifest: SilverManifest = from_dict(
            SilverManifest,
            manifest_raw,
            config=Config(cast=[str, Env, ModelType, DomainSource, ETLLayer, ReferenceSource])
        )

        # Summary mapping
        summary_raw['env'] = Env(summary_raw.get('env', 'unknown'))
        summary_raw['etl_layer'] = ETLLayer.SILVER
        for r in summary_raw.get("results", []):
            r['domain_source'] = DomainSource(r.get("domain_source", "UNKNOWN"))
            r['domain_source_type'] = DomainSourceType(r.get("domain_source_type", "unknown"))

        summary: SilverSummary = from_dict(
            SilverSummary,
            summary_raw,
            config=Config(
                cast=[Env, ETLLayer, DomainSource, DomainSourceType],
                type_hooks={datetime.datetime: datetime.datetime.fromisoformat}
            )
        )
        
        summary.correlation_id = summary_raw.get("correlation_id", "UNKNOWN")

        context = SilverContext(
            env=manifest.env,
            etl_layer=ETLLayer.SILVER,
            manifest=manifest,
            summary=summary,
            correlation_id=summary.correlation_id
        )

        return context