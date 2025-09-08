from dataclasses import dataclass, field
from typing import List, Optional
from src.common.enums.model_type import ModelType
from src.common.enums.model_type_table import ModelTypeTable
from src.common.models.manifest import ManifestBase
from src.common.models.models import SummaryBase, SummaryResultBase


@dataclass(kw_only=True)
class GoldSummaryResult(SummaryResultBase):
    model: ModelType
    output_path: str
    operation_type: str = ""
    record_count: int = 0

@dataclass
class GoldSummary(SummaryBase):
    # Pola bez wartości domyślnej muszą być na początku
    processed_models: int = 0
    
    # Pola z wartością domyślną dziedziczone z SummaryBase
    # lub zdefiniowane bezpośrednio w klasie
    results: List[GoldSummaryResult] = field(default_factory=list)

@dataclass
class GoldManifestModel:
    name: str
    source_models: List[ModelType]       # np. ["COUNTRY", "POPULATION"]
    primary_keys: List[str]
    type: ModelTypeTable

@dataclass
class GoldManifest(ManifestBase):
    models: List[GoldManifestModel]


