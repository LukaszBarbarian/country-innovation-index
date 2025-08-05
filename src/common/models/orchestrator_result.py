# src/common/models/orchestrator_result.py
from dataclasses import dataclass, field
from typing import Optional, Any, Dict
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env

@dataclass
class OrchestratorResult:
    status: str
    correlation_id: str
    queue_message_id: str    
    layer_name: ETLLayer
    env: Env
    message: str
    api_name: Optional[str] = None
    dataset_name: Optional[str] = None
    output_path: Optional[str] = None
    api_response_status_code: Optional[int] = None
    error_details: Optional[Dict[str, Any]] = field(default_factory=dict) 

    @property
    def is_success(self) -> bool:
        return self.status == "COMPLETED"
    
    @property
    def is_failed(self) -> bool:
        return self.status == "FAILED"