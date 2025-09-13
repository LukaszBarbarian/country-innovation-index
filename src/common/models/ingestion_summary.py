from dataclasses import dataclass
from typing import List
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.models.ingestion_result import IngestionResult


@dataclass(frozen=True)
class IngestionSummary:
    """
    A dataclass representing the summary file for an entire Bronze layer run.
    """
    status: str
    env: Env
    etl_layer: ETLLayer
    correlation_id: str
    timestamp: str
    processed_items: int
    results: List[IngestionResult]

    @property
    def overall_status(self) -> str:
        """
        Calculates the overall status based on the individual results.

        If there are no results, it returns "NO_RESULTS". If all individual results are valid,
        it returns "COMPLETED". If at least one result is not valid, it returns
        "PARTIAL_SUCCESS". If all results are invalid, it returns "FAILED".
        """
        if not self.results:
            return "NO_RESULTS"

        if all(result.is_valid for result in self.results):
            return "COMPLETED"
        elif any(not result.is_valid for result in self.results):
            return "PARTIAL_SUCCESS"
        else:
            return "FAILED"