from dataclasses import dataclass
from typing import Any, Union
from src.common.enums.file_format import FileFormat

@dataclass
class ProcessedResult:
    data: Any
    format: FileFormat
    metadata: dict = None
    blob_name: str = ""

