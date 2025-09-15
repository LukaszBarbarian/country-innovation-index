from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class SourceConfigPayload:
    """
    A dataclass representing the configuration payload for a single data source.
    It specifies the type of source, its domain, the dataset name, and the
    request parameters needed to access the data.
    """
    domain_source_type: DomainSourceType
    domain_source: DomainSource
    dataset_name: str
    request_payload: Dict[str, Any]


@dataclass
class DataSource:
    """
    A dataclass that acts as a wrapper for a SourceConfigPayload.
    It encapsulates the configuration for a single data source within a pipeline.
    """
    source_config_payload: SourceConfigPayload
    

@dataclass
class PipelineConfig:
    """
    A dataclass representing the complete configuration for an ETL pipeline.
    It contains high-level information about the environment and ETL layer,
    as well as a list of all data sources to be processed.
    """
    env: Env
    etl_layer: ETLLayer
    sources: List[DataSource]


@dataclass
class ManifestBase:
    """
    An abstract base dataclass for data manifests.
    It serves as a foundational class for specific manifest types,
    ensuring they all include environment and ETL layer information.
    """
    env: Env
    etl_layer: ETLLayer