from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime


@dataclass
class RawData:
    """
    A dataclass that serves as a universal container for raw data ingested from a source.
    It encapsulates the data itself along with essential metadata for tracking its origin.
    """
    data: Any
    dataset_name: Optional[str] = None
    source_timestamp_utc: Optional[datetime] = None
    metadata: Optional[dict] = None