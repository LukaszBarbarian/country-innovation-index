# src/common/models/orchestrator_result.py

from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List
import datetime
from pydantic import BaseModel, Field
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env

class OrchestratorResult(BaseModel):
    status: str
    correlation_id: str
    layer_name: ETLLayer
    env: Env
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    processed_items: int
    duration_in_ms: int
    summary_url: str
    message: str
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