# src/silver/manifest/silver_manifest_parser.py
from typing import Dict, List, Any

from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.manifest.manifest_parser import BaseManifestParser
from src.common.enums.model_type import ModelType
from src.common.models.manifest import ManualDataPath, Model
from src.silver.manifest.silver_manifest import SilverManifest


class SilverManifestParser(BaseManifestParser):
    @staticmethod
    def _parse_models(models_data: List[Dict[str, Any]]) -> List[Model]:
        """Parsuje listę modeli, konwertując nazwy modeli na typy enuma."""
        parsed_models = []
        for model in models_data:
            try:
                model['model_name'] = ModelType(model['model_name'])
                parsed_models.append(Model(**model))
            except (KeyError, ValueError) as e:
                raise ValueError(f"Błąd parsowania modelu: {e}. Dane: {model}")
        return parsed_models

    @staticmethod
    def _parse_manual_data_paths(paths_data: List[Dict[str, str]]) -> List[ManualDataPath]:
        """Parsuje listę ścieżek do danych manualnych."""
        return [ManualDataPath(**path) for path in paths_data]

    @staticmethod
    def parse(manifest_data: Dict) -> SilverManifest:
        try:
            etl_layer_str = manifest_data.get("etl_layer")
            if not etl_layer_str or etl_layer_str != "silver":
                raise ValueError("Błąd parsowania: manifest nie jest typu silver.")

            parsed_models = SilverManifestParser._parse_models(
                manifest_data.get("models", [])
            )
            parsed_manual_paths = SilverManifestParser._parse_manual_data_paths(
                manifest_data.get("manual_data_paths", [])
            )
            
            return SilverManifest(
                env=Env(manifest_data['env']),
                etl_layer=ETLLayer(etl_layer_str),
                references_tables=manifest_data.get('references_tables', {}),
                manual_data_paths=parsed_manual_paths,
                models=parsed_models
            )
        except (KeyError, ValueError) as e:
            raise ValueError(f"Błąd parsowania manifestu Silver: {e}")