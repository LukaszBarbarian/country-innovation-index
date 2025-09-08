import json
from typing import Optional
from dacite import Config, from_dict
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.models.manifest import BronzeManifest
from src.common.contexts.base_parser import BaseParser
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer


class BronzeParser(BaseParser):
    def parse(self, manifest_json: str, summary_json: Optional[str] = None) -> BronzeContext:

        # --- manifest mapping ---
        manifest_json['env'] = Env(manifest_json.get('env', 'unknown'))

        for src in manifest_json.get("sources", []):
            scp = src.get("source_config_payload", {})
            scp["domain_source_type"] = DomainSourceType(scp.get("domain_source_type", "unknown"))
            scp["domain_source"] = DomainSource(scp.get("domain_source", "UNKNOWN"))

        manifest: BronzeManifest = from_dict(
            BronzeManifest,
            manifest_json,
            config=Config(cast=[str, Env, DomainSource, DomainSourceType, ETLLayer])
        )


        # --- context ---
        context = BronzeContext(
            env=manifest.env,
            etl_layer=ETLLayer.BRONZE,
            manifest=manifest
        )        

        return context
