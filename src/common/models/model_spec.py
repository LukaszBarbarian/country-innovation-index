from dataclasses import dataclass
from typing import List, Optional
from src.common.enums.model_type import ModelType
from src.common.enums.model_type_table import ModelTypeTable


@dataclass
class ModelSpec:
    """
    A dataclass representing the specification of a Gold layer model, typically
    defined in a manifest file. It contains all the necessary information to
    build a model, including its name, type, source dependencies, and table details.
    """
    name: str
    type: ModelTypeTable  # "DIM" or "FACT"
    source_models: List[ModelType]
    table_name: str
    primary_keys: Optional[List[str]] = None