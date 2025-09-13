from dataclasses import dataclass
import datetime
from typing import Dict, List, Optional

from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer


@dataclass
class SummaryResultBase:
    """
    A base dataclass for representing the result of a single task or sub-process
    within a larger ETL run. It stores key metrics like status and duration.
    """
    correlation_id: str
    status: str
    duration_in_ms: int
    timestamp: Optional[datetime.datetime] = None
    error_details: Optional[Dict] = None

    def is_valid(self) -> bool:
        """
        Returns True if the status is 'COMPLETED', otherwise False.
        The check is case-insensitive.
        """
        # We'll use `upper()` just in case to ensure consistency
        return self.status.upper() == "COMPLETED"

@dataclass
class SummaryBase:
    """
    A base dataclass representing a high-level summary of an entire ETL job.
    It aggregates the results from multiple sub-tasks and provides an
    overall status for the entire run.
    """
    status: str
    env: Env
    etl_layer: ETLLayer
    correlation_id: str
    results: List[SummaryResultBase]
    timestamp: Optional[datetime.datetime] = None

    def is_valid(self) -> bool:
        """
        Returns True if the summary status is 'COMPLETED' and all individual
        results in the 'results' list are also valid.
        """
        is_summary_valid = self.status.upper() == "COMPLETED"
        if not is_summary_valid:
            return False

        # Check each object in the results list
        return all(result.is_valid() for result in self.results)