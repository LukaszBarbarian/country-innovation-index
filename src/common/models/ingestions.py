from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


# src/common/models/ingestion_models.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import datetime
from src.common.models.base_context import BaseContext
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.models.manifest import SourceConfigPayload

@dataclass(frozen=True)
class IngestionResult:
    correlation_id: str
    env: str
    etl_layer: str
    domain_source: str
    domain_source_type: str
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


@dataclass(frozen=True)
class IngestionSummary:
    """Klasa dla całego pliku podsumowania po warstwie Bronze."""
    status: str
    env: str
    etl_layer: str
    correlation_id: str
    timestamp: str
    processed_items: int
    results: List[IngestionResult]



@dataclass(frozen=True, kw_only=True)
class IngestionContext(BaseContext):
    source_config: SourceConfigPayload

    


    