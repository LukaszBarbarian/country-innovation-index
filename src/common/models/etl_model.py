from dataclasses import dataclass, field
from pyspark.sql import DataFrame
from datetime import datetime
from typing import Dict, Any, Optional

from src.common.enums.model_type import ModelType


@dataclass
class EtlModel():
    data: DataFrame
    model_type: ModelType
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "SUCCESS"

