from dataclasses import dataclass, field
from pyspark.sql import DataFrame
from datetime import datetime
from typing import Dict, Any, List, Optional

from src.common.enums.model_type import ModelType


@dataclass
class EtlModel():
    data: DataFrame
    type: ModelType
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "SUCCESS"

    partition_cols: Optional[List[str]] = field(default_factory=list)

