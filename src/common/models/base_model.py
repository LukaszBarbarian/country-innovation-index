from dataclasses import dataclass, field
from pyspark.sql import DataFrame
from datetime import datetime
from typing import Dict, Any, Optional


@dataclass
class BaseModel():
    data: DataFrame
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "SUCCESS"

