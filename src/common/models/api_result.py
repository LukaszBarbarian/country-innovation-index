from abc import ABC
from dataclasses import dataclass, field
import datetime
from typing import Dict, List, Any, Optional


@dataclass
class ApiResult(ABC):
    records: List[Any]
    endpoint: str
    url: str
    request_payload: Dict[str, Any]
    fetched_at: datetime