from dataclasses import dataclass, field
from typing import Any, Dict
from src.common.enums.file_format import FileFormat

@dataclass
class ProcessedResult:
    """
    A dataclass that serves as a container for the result of a data processing step.
    It holds the processed data itself, its format, and associated metadata.
    """
    data: Any
    format: FileFormat
    metadata: Dict[str, Any] = field(default_factory=dict)
    blob_name: str = ""
    skip_upload: bool = False