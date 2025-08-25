from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import List, Dict, Any

from src.common.enums import domain_source
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.model_type import ModelType


from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class SourceConfigPayload:
    domain_source_type: DomainSourceType
    domain_source: DomainSource
    dataset_name: str
    request_payload: Dict[str, Any]


@dataclass
class DataSource:
    source_config_payload: SourceConfigPayload
    

@dataclass
class SourceConfigPayload:
    domain_source_type: DomainSourceType
    domain_source: DomainSource
    dataset_name: str
    request_payload: Dict[str, Any]

@dataclass
class DataSource:
    source_config_payload: SourceConfigPayload

@dataclass
class PipelineConfig:
    env: Env
    etl_layer: ETLLayer
    sources: List[DataSource]



@dataclass(frozen=True)
class ModelSourceItem:
    domain_source: DomainSource
    dataset_name: str

@dataclass(frozen=True)
class ManualDataPath:
    domain_source: DomainSource
    dataset_name: str
    file_path: str

@dataclass(frozen=True)
class Model:
    model_name: ModelType
    table_name: str
    source_datasets: List[ModelSourceItem]
    depends_on: List[ModelType] = field(default_factory=list)
    
@dataclass(frozen=True)
class SilverManifest:
    env: Env
    etl_layer: ETLLayer
    references_tables: Dict[str, str]
    manual_data_paths: List[ManualDataPath]
    models: List[Model]


