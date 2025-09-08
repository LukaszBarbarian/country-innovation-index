from dataclasses import dataclass
import datetime
from typing import Dict, List, Optional

from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer


@dataclass
class SummaryResultBase:
    correlation_id: str
    status: str
    duration_in_ms: int
    timestamp: Optional[datetime.datetime] = None
    error_details: Optional[Dict] = None


    def is_valid(self) -> bool:
        """
        Zwraca True, jeśli status to 'COMPLETED', w przeciwnym razie False.
        """
        # Użyjemy `upper()` na wszelki wypadek, aby zapewnić spójność
        return self.status.upper() == "COMPLETED"

@dataclass
class SummaryBase:
    status: str
    env: Env
    etl_layer: ETLLayer
    correlation_id: str
    results: List[SummaryResultBase]
    timestamp: Optional[datetime.datetime] = None



    def is_valid(self) -> bool:
        """
        Zwraca True, jeśli status to 'COMPLETED'
        oraz wszystkie wyniki na liście 'results' są ważne.
        """
        is_summary_valid = self.status.upper() == "COMPLETED"
        if not is_summary_valid:
            return False

        # Sprawdź każdy obiekt na liście results
        return all(result.is_valid() for result in self.results)
    
