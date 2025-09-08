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
    domain_source: DomainSource
    domain_source_type: DomainSourceType
    dataset_name: str
    message: str
    output_paths: List[str]
    full_output_paths: Optional[List[str]] = None


@dataclass
class SilverSummary(SummaryBase):
    processed_items: int = 0
    results: List[SilverSummaryResult] = field(default_factory=list)

@dataclass
class SilverManifestSourceDataset:
    domain_source: DomainSource
    dataset_name: str

@dataclass
class SilverManifestModel:
    model_name: ModelType
    table_name: str
    source_datasets: List[SilverManifestSourceDataset]
    depends_on: Optional[List[ModelType]] = field(default_factory=list)

@dataclass
class ManualDataPath:
    domain_source: DomainSource
    dataset_name: str
    file_path: str

@dataclass
class SilverManifest(ManifestBase):
    references_tables: Dict[ReferenceSource, str]
    manual_data_paths: List[ManualDataPath]
    models: List[SilverManifestModel]
