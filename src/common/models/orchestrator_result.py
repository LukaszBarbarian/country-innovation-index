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
    api_name: str
    dataset_name: str
    layer_name: ETLLayer
    env: Env
    message: str
    output_path: Optional[str] = None  # Zmieniono z bronze_output_path
    api_response_status_code: Optional[int] = None # Może być nadal istotne dla debugowania nawet w kolejnych warstwach
    error_details: Optional[Dict[str, Any]] = field(default_factory=dict) 

    @property
    def is_success(self) -> bool:
        return self.status == "COMPLETED"
    
    @property
    def is_failed(self) -> bool:
        return self.status == "FAILED"