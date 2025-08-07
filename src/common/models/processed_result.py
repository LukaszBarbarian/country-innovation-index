from dataclasses import dataclass, field
from typing import Any, Dict
from src.common.enums.file_format import FileFormat

@dataclass
class ProcessedResult:
    data: Any
    format: FileFormat
    metadata: Dict[str, Any] = field(default_factory=dict)
    blob_name: str = ""
    skip_upload: bool = False
