from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import List, Dict, Any

from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.model_type import ModelType

@dataclass(frozen=True, kw_only=True)
class ManifestBase:
    env: Env
    etl_layer: ETLLayer



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
class RequestPayload:
    """Klasa bazowa dla ładunków żądań."""
    pass

@dataclass(frozen=True)
class ApiRequestPayload(RequestPayload):
    """Payload dla źródła typu 'api'."""
    offset: int
    limit: int

@dataclass(frozen=True)
class StaticFileRequestPayload(RequestPayload):
    """Payload dla źródła typu 'static_file'."""
    file_path: List[str]

@dataclass(frozen=True)
class SourceConfigPayload(ManifestBase):
    """Konfiguracja pojedynczego źródła danych w manifeście Bronze."""
    domain_source_type: DomainSourceType
    domain_source: DomainSource
    dataset_name: str
    request_payload: Dict[str, Any]



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
    output_blob_path: str