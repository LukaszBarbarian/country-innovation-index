from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime


@dataclass
class RawData:
    data: Any  # np. dict z wartościami ze źródła
    dataset_name: Optional[str] = None  # z jakiego datasetu pochodzi (np. "nobelPrizes")
    source_timestamp_utc: Optional[datetime] = None  # jeśli źródło miało znacznik czasu
    metadata: Optional[dict] = None  # dowolne inne info np. numer wiersza, uuid, etc.
