from typing import Dict
from src.bronze.manifest.bronze_manifest import BronzeManifest
from src.common.enums.etl_layers import ETLLayer
from src.common.manifest.manifest_parser import BaseManifestParser
from src.common.models.manifest import SourceConfigPayload
from src.common.registers.manifest_registry import ManifestParserRegistry


@ManifestParserRegistry.register(ETLLayer.BRONZE)
class BronzeManifestParser(BaseManifestParser):
    @staticmethod
    def parse(manifest_data: Dict) -> BronzeManifest:
        """
        Parsuje cały manifest JSON do obiektu BronzeManifest.
        
        Parametry:
            manifest_data (Dict): Pełny słownik manifestu z kluczami 'env', 'etl_layer' i 'sources'.
                                        
        Zwraca:
            BronzeManifest: Obiekt manifestu z przetworzonymi danymi.
        """
        try:
            # Sprawdzamy wymagane klucze w manifeście
            env = manifest_data['env']
            etl_layer_str = manifest_data['etl_layer']
            sources_list = manifest_data['sources']

            # Upewniamy się, że 'sources' to lista
            if not isinstance(sources_list, list):
                raise ValueError("Klucz 'sources' musi być listą.")

            # Parsujemy listę źródeł
            sources = [SourceConfigPayload(**source['source_config_payload'])
                       for source in sources_list]
            
            return BronzeManifest(
                env=env,
                etl_layer=etl_layer_str,
                sources=sources
            )
        except KeyError as e:
            # Błąd, jeśli brakuje kluczy 'env', 'etl_layer' lub 'sources'
            raise ValueError(f"Brak wymaganego klucza w manifeście: {e}")
        except Exception as e:
            # Obsługa innych błędów parsowania
            raise ValueError(f"Błąd podczas parsowania manifestu: {e}")