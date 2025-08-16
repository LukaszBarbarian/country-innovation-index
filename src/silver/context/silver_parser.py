# src/silver/parsers/silver_payload_parser.py
import datetime
from typing import Dict, Any, List
import uuid

from src.common.contexts.base_parser import BaseParser
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.silver.context.silver_context import SilverLayerContext
from src.silver.manifest.silver_manifest_parser import SilverManifestParser


class SilverPayloadParser(BaseParser):
    def __init__(self, manifest_loader):
        # ManifestLoader jest potrzebny, aby pobrać i sparsować manifest.
        # W praktyce, to on pobierze plik z magazynu.
        self._manifest_loader = manifest_loader

    async def parse(self, payload: Dict[str, Any]) -> SilverLayerContext:
        """Parsuje wejściowy payload wiadomości i tworzy SilverLayerContext."""
        try:
            # 1. Walidacja i parsowanie pól z payloadu
            correlation_id = payload.get("correlation_id", str(uuid.uuid4()))
            layer_str = payload.get("layer")
            env_str = payload.get("env")
            bronze_output_uri = payload.get("bronze_output_uri")
            manifest_path = payload.get("manifest")
            
            # Weryfikacja wymaganych pól
            if not all([layer_str, env_str, bronze_output_uri, manifest_path]):
                raise ValueError("Brak wymaganych kluczy w payloadzie: 'layer', 'env', 'bronze_output_uri', 'manifest'.")

            # Weryfikacja poprawności wartości pól
            if layer_str != "bronze":
                raise ValueError("Payload musi pochodzić z warstwy 'bronze'.")
            
            # 2. Parsowanie enums
            etl_layer = ETLLayer.SILVER
            env = Env(env_str)

            # 3. Parsowanie manifestu Silver
            manifest_data = await self._manifest_loader.load_manifest(manifest_path)
            silver_manifest: SilverManifest = SilverManifestParser.parse(manifest_data)

            # 4. Tworzenie i zwracanie obiektu kontekstu
            silver_context = SilverLayerContext(
                correlation_id=correlation_id,
                etl_layer=etl_layer,
                env=env,
                bronze_output_uri=bronze_output_uri,
                manifest_object=silver_manifest,
                is_valid=True
            )
            
            return silver_context

        except KeyError as e:
            raise ValueError(f"Błąd parsowania payloadu: brak klucza {e}.")
        except ValueError as e:
            raise ValueError(f"Nieprawidłowa wartość w payloadzie: {e}")
        except Exception as e:
            # Ogólna obsługa błędów, jeśli coś pójdzie nie tak (np. brak pliku manifestu)
            raise RuntimeError(f"Błąd podczas tworzenia kontekstu Silver: {e}")