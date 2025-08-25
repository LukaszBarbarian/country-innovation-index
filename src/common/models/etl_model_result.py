# src/silver/models/etl_model_result.py

from dataclasses import dataclass
from typing import Optional, Dict, Any

from src.common.models.etl_model import EtlModel
from src.common.models.manifest import Model

@dataclass
class EtlModelResult:
    """
    Reprezentuje wynik przetwarzania pojedynczego modelu w warstwie Silver.
    """
    model: EtlModel
    status: str
    output_path: str
    correlation_id: str
    timestamp: str
    operation_type: str # Np. 'CREATE', 'UPDATE', 'UPSERT'
    record_count: Optional[int] = None
    error_details: Optional[Dict[str, Any]] = None
    duration_in_ms: Optional[int] = None