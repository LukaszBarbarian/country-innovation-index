from dataclasses import dataclass
from typing import List
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.models.ingestion_result import IngestionResult


@dataclass(frozen=True)
class IngestionSummary:
    """Klasa dla całego pliku podsumowania po warstwie Bronze."""
    status: str
    env: Env
    etl_layer: ETLLayer
    correlation_id: str
    timestamp: str
    processed_items: int
    results: List[IngestionResult]

    @property
    def overall_status(self) -> str:
        """Oblicza ogólny status na podstawie wszystkich wyników."""
        if not self.results:
            return "NO_RESULTS"

        if all(result.is_valid for result in self.results):
            return "COMPLETED"
        elif any(not result.is_valid for result in self.results):
            return "PARTIAL_SUCCESS"
        else:
            return "FAILED"    
