from dataclasses import asdict, dataclass, field
from enum import Enum
import os
from typing import Any, Dict, List, Optional

# src/common/models/ingestion_models.py

from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.models.base_process_result import BaseProcessResult

@dataclass
class IngestionResult(BaseProcessResult):
    """
    A dataclass representing the result of an ingestion process.

    This class extends `BaseProcessResult` and adds methods to handle
    the specific needs of ingestion results, such as converting to a
    dictionary and validating the status.
    """
    pass

    def to_dict(self) -> dict[str, Any]:
        """
        Converts the dataclass object into a dictionary, handling Enum types.
        
        This method recursively traverses the dataclass and its nested fields,
        converting any Enum instances to their value representation.
        
        Returns:
            dict[str, Any]: The dictionary representation of the dataclass.
        """
        def convert_value(obj):
            if isinstance(obj, Enum):
                return obj.value
            if isinstance(obj, dict):
                return {k: convert_value(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert_value(item) for item in obj]
            return obj

        return convert_value(asdict(self))
    
    @property
    def is_valid(self) -> bool:
        """
        Returns True if the status indicates a successful completion, otherwise False.
        """
        return self.status in ["COMPLETED", "SUCCESS"]


    def update_paths(self, base_path: str):
        """
        Updates the output paths with a base path.
        
        This method prepends a given base path to all entries in the `output_paths` list,
        ensuring they form complete, valid file paths.
        
        Args:
            base_path (str): The base path to prepend.
        """
        self.output_paths = [f"{base_path}/{path.strip('/')}" for path in self.output_paths]