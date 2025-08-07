# src/common/models/ingestion_result.py

from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List

@dataclass
class IngestionResult:
    """
    Wynik operacji pozyskiwania danych. Zawiera informacje o sukcesie/porażce
    i listę ścieżek do plików, które zostały utworzone.
    """
    status: str
    message: str
    output_paths: Optional[List[str]] = None
    source_response_status_code: Optional[int] = None
    error_details: Optional[Dict[str, Any]] = field(default_factory=dict)
    
    @property
    def is_success(self) -> bool:
        return self.status == "COMPLETED"
    
    @property
    def is_failed(self) -> bool:
        return self.status == "FAILED"