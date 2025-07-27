from src.common.enums.file_format import FileFormat
from typing import Any, Union
import json

class FormatSerializer:
    @staticmethod
    def serialize(data: Any, fmt: FileFormat) -> Union[str, bytes]:
        match fmt:
            case FileFormat.JSON:
                return json.dumps(data, indent=2)
            case FileFormat.CSV:
                return data  # np. już wygenerowany CSV jako string
            case FileFormat.BINARY:
                return data  # zakładamy bytes
            case _:
                raise ValueError(f"Nieobsługiwany format: {fmt}")
