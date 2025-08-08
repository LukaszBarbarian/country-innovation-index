from typing import Dict, Any, List, cast
from dataclasses import dataclass, field

@dataclass
class ReferenceDataItem:
    domain_source: str
    dataset_name: str
    file_path: str

@dataclass
class ReferenceConfig:
    reference_data_paths: List[ReferenceDataItem] = field(default_factory=list)
    references: Dict[str, str] = field(default_factory=dict)

# Parser for reference configuration
class ReferenceConfigParser:
    def parse(self, payload: Dict[str, Any]) -> ReferenceConfig:
        if "reference_data_paths" not in payload:
            return ReferenceConfig()
        
        parsed_data_paths = [
            ReferenceDataItem(
                domain_source=cast(str, item["domain_source"]),
                dataset_name=cast(str, item["dataset_name"]),
                file_path=cast(str, item["file_path"])
            ) for item in cast(List[Dict[str, str]], payload["reference_data_paths"])
        ]
        
        references = cast(Dict[str, str], payload.get("references", {}))
        
        return ReferenceConfig(
            reference_data_paths=parsed_data_paths,
            references=references
        )