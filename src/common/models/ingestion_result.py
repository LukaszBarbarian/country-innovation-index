from dataclasses import asdict, dataclass
from enum import Enum
import os
from typing import Any, Dict, List, Optional


# src/common/models/ingestion_models.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional

from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer

@dataclass
class IngestionResult:
    correlation_id: str
    env: Env
    etl_layer: ETLLayer
    domain_source: DomainSource
    domain_source_type: DomainSourceType
    dataset_name: Optional[str] = None
    status: str = "PENDING"
    message: Optional[str] = None
    output_paths: List[str] = field(default_factory=list)
    error_details: Dict[str, Any] = field(default_factory=dict)
    
    duration_in_ms: int = 0

    
    def to_dict(self) -> dict[str, Any]:
            """Konwertuje obiekt dataclass na słownik, obsługując enumy."""
            def convert_value(obj):
                if isinstance(obj, Enum):
                    return obj.value
                if isinstance(obj, dict):
                    return {k: convert_value(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [convert_value(item) for item in obj]
                return obj

            return convert_value(asdict(self))
    
    @property
    def is_valid(self) -> bool:
        """
        Zwraca True, jeśli status jest pomyślny, w przeciwnym razie False.
        """
        return self.status in ["COMPLETED", "SUCCESS"]


    def update_paths(self, base_path: str):
        self.output_paths = [f"{base_path}/{path.strip('/')}" for path in self.output_paths]






    


    