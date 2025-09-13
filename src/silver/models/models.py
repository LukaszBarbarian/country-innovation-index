from dataclasses import dataclass, field
from typing import List, Optional, Dict
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.model_type import ModelType
from src.common.enums.reference_source import ReferenceSource
from src.common.models.manifest import ManifestBase
from src.common.models.models import SummaryBase, SummaryResultBase


@dataclass(kw_only=True)
class SilverSummaryResult(SummaryResultBase):
    """
    Represents the detailed outcome of a single dataset's processing within the Silver ETL layer.
    """
    domain_source: DomainSource
    domain_source_type: DomainSourceType
    dataset_name: str
    message: str
    output_paths: List[str]
    full_output_paths: Optional[List[str]] = None


@dataclass
class SilverSummary(SummaryBase):
    """
    A summary of the entire Silver ETL pipeline run, including the count of
    processed items and a list of individual results.
    """
    processed_items: int = 0
    results: List[SilverSummaryResult] = field(default_factory=list)


@dataclass
class SilverManifestSourceDataset:
    """
    Defines a single source dataset required for building a Silver model.
    """
    domain_source: DomainSource
    dataset_name: str


@dataclass
class SilverManifestModel:
    """
    Defines a single analytical model to be built in the Silver layer.

    It specifies the model's name, its target table name, its required source
    datasets, and any dependencies on other Silver models.
    """
    model_name: ModelType
    table_name: str
    source_datasets: List[SilverManifestSourceDataset]
    depends_on: Optional[List[ModelType]] = field(default_factory=list)


@dataclass
class ManualDataPath:
    """
    Specifies the location of a dataset that is manually uploaded and not from
    a standard Bronze data source.
    """
    domain_source: DomainSource
    dataset_name: str
    file_path: str


@dataclass
class SilverManifest(ManifestBase):
    """
    The main manifest for the Silver ETL layer.

    This manifest contains the configuration for all data products to be created,
    including paths to reference and manual data, and the specifications for
    each analytical model.
    """
    references_tables: Dict[ReferenceSource, str]
    manual_data_paths: List[ManualDataPath]
    models: List[SilverManifestModel]