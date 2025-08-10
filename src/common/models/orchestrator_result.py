# src/common/models/orchestrator_result.py
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from pydantic import BaseModel, Field


class OrchestratorResult(BaseModel):
    status: str
    correlation_id: str
    layer_name: ETLLayer
    env: Env
    message: str
    domain_source: DomainSource = DomainSource.UNKNOWN
    domain_source_type: DomainSourceType = DomainSourceType.UNKNOWN
    dataset_name: Optional[str] = None
    output_paths: Optional[List[str]] = None
    source_response_status_code: Optional[int] = None
    error_details: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        return self.status in ["COMPLETED", "SKIPPED"]
    
    @property
    def is_failed(self) -> bool:
        return self.status == "FAILED"
    

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(
            mode='json',
            exclude_none=True
        )