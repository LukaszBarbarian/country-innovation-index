from dataclasses import dataclass
from typing import List, Optional
from src.common.enums.model_type import ModelType
from src.common.enums.model_type_table import ModelTypeTable


@dataclass
class ModelSpec:
    """
    Specyfikacja modelu Gold (pochodzi z manifestu).
    source_models: lista ModelType (np. [ModelType.COUNTRY, ModelType.POPULATION])
    """
    name: str
    type: ModelTypeTable                       # "DIM" lub "FACT" (string dla prostoty)
    source_models: List[ModelType]
    table_name: str
    primary_keys: Optional[List[str]] = None