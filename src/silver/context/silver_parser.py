
from src.common.contexts.base_parser import BaseParser
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.models.ingestions import IngestionResult, IngestionSummary
from src.common.models.manifest import ManualDataPath, Model
from src.silver.context.silver_context import SilverLayerContext


class SilverPayloadParser(BaseParser):
    def parse(self, manifest_payload, payload = None):
        try:
            # 1. Parsowanie danych podsumowania z payloadu
            ingestion_results = [IngestionResult(**result) for result in payload.get('results', [])]
            ingestion_summary = IngestionSummary(
                status=payload['status'],
                env=payload['env'],
                etl_layer=payload['etl_layer'],
                correlation_id=payload['correlation_id'],
                timestamp=payload['timestamp'],
                processed_items=payload['processed_items'],
                results=ingestion_results
            )

            # 2. Parsowanie danych z manifestu
            references_tables = manifest_payload.get('references_tables', {})
            manual_data_paths = [
                ManualDataPath(**item)
                for item in manifest_payload.get('manual_data_paths', [])
            ]
            models = [
                Model(**item)
                for item in manifest_payload.get('models', [])
            ]


            # 3. Zwrócenie instancji kontekstu z sparsowanymi danymi
            return SilverLayerContext(
                references_tables=references_tables,
                manual_data_paths=manual_data_paths,
                models=models,
                ingestion_summary=ingestion_summary,
                # Atrybuty z BaseContext
                etl_layer=ETLLayer(manifest_payload['etl_layer']),
                env=Env(manifest_payload['env']),
                correlation_id=payload['correlation_id'],
                is_valid=self._validate_context(ingestion_summary)
            )

        except KeyError as e:
            raise ValueError(f"Błąd danych: brak wymaganego klucza {e}")
        except TypeError as e:
            raise ValueError(f"Błąd typowania danych: {e}")
        except Exception as e:
            raise ValueError(f"Ogólny błąd tworzenia kontekstu: {e}")
        


    def _validate_context(self, summary: IngestionSummary) -> bool:
        """
        Waliduje kontekst na podstawie statusów w IngestionSummary.
        Zwraca True, jeśli status podsumowania to "COMPLETED" lub "PARTIAL_SUCCESS",
        oraz jeśli żaden z wyników nie ma statusu "FAILED".
        """
        
        # 1. Sprawdzenie statusu całego podsumowania
        if summary.status not in ["COMPLETED", "PARTIAL_SUCCESS"]:
            return False
        
        # 2. Sprawdzenie statusu każdego z wyników
        # Użycie `any` jest bardziej wydajne i czytelne
        if any(result.status == "FAILED" for result in summary.results):
            return False
            
        return True