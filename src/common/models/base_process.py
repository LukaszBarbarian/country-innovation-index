from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame


@dataclass
class BaseProcessModel:
    """
    A dataclass that represents a processed data model.

    This class serves as a container for a Spark DataFrame and its associated
    metadata, making it easier to pass the processed data through an ETL pipeline.
    """
    data: DataFrame
    metadata: Dict[str, Any] = field(default_factory=dict)
    name: str = ""
    partition_cols: Optional[List[str]] = None