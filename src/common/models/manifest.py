from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import List, Dict, Any

from src.common.enums.model_type import ModelType


from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class SourceConfigPayload:
    domain_source_type: str
    domain_source: str
    dataset_name: str
    request_payload: Dict[str, Any]


@dataclass
class DataSource:
    source_config_payload: SourceConfigPayload


@dataclass
class PipelineConfig:
    env: str
    etl_layer: str
    sources: List[DataSource]



@dataclass(frozen=True)
class ManualDataPath:
    """Reprezentuje pojedynczy plik z danymi w sekcji manual_data_paths."""
    domain_source: str
    dataset_name: str
    file_path: str

@dataclass(frozen=True)
class Model:
    """Reprezentuje pojedynczy model danych w manifeście Silver."""
    model_name: ModelType
    source_datasets: List[str]
    status: str
    errors: List[Any]