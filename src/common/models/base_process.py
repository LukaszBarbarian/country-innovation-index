from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame


@dataclass
class BaseProcessModel:
    data: DataFrame
    metadata: Dict[str, Any] = field(default_factory=dict)
    name: str = ""
    partition_cols: Optional[List[str]] = None


    