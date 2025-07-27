from dataclasses import dataclass
from typing import Any, Union
from src.common.enums.file_format import FileFormat
from src.functions.common.processors.processed_result_serializer import FormatSerializer

@dataclass
class ProcessedResult:
    data: Any
    format: FileFormat
    metadata: dict = None
    blob_name: str

    def serialize(self) -> Union[str, bytes]:
        FormatSerializer.serialize(self, self.format)