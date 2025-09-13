# src/common/models/base_process_result.py

from dataclasses import dataclass, field
import datetime
from typing import Any, Dict, List, Optional
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType

@dataclass
class BaseProcessResult:
    """
    A base class for storing the results of a data processing operation.
    It's a dataclass that provides a structured way to report on the outcome
    of a process, including status, duration, and any associated metadata.
    """
    status: str
    correlation_id: str
    duration_in_ms: int = 0  # Setting a default value
    record_count: int = 0

    # Fields moved from IngestionResult
    domain_source: Optional[DomainSource] = None
    domain_source_type: Optional[DomainSourceType] = None
    dataset_name: Optional[str] = None
    message: Optional[str] = None
    output_paths: List[str] = field(default_factory=list)

    start_time: Optional[datetime.datetime] = datetime.datetime.utcnow().isoformat()
    end_time: Optional[datetime.datetime] = None

    error_details: Dict[str, Any] = field(default_factory=dict)