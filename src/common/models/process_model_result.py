from dataclasses import dataclass
from typing import List, Optional

from src.common.models.base_process_result import BaseProcessResult


@dataclass
class ProcessModelResult(BaseProcessResult):
    """
    A dataclass representing the result of a model processing operation for the
    Silver and Gold layers. It inherits from `BaseProcessResult` and adds
    fields specific to model-building tasks.
    """
    model: Optional[str] = None
    operation_type: Optional[str] = None
    output_path: Optional[str] = None