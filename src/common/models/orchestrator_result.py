# src/common/models/orchestrator_result.py

from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List
import datetime
from pydantic import BaseModel, Field
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env

class OrchestratorResult(BaseModel):
    """
    A Pydantic model representing the result of an orchestration process.
    It provides a structured and validated way to report the outcome of a
    complete ETL job for a specific layer.
    """
    status: str
    correlation_id: str
    etl_layer: ETLLayer
    env: Env
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    processed_items: int
    duration_in_ms: int
    summary_url: Optional[str] = None
    message: str
    error_details: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        """
        Checks if the orchestration process was successful.
        A process is considered successful if its status is "COMPLETED" or "SKIPPED".
        """
        return self.status in ["COMPLETED", "SKIPPED"]
    
    @property
    def is_failed(self) -> bool:
        """
        Checks if the orchestration process has failed.
        """
        return self.status == "FAILED"
    
    def to_dict(self) -> dict[str, Any]:
        """
        Converts the OrchestratorResult model into a dictionary.
        Pydantic's `model_dump` method is used to ensure a clean, JSON-serializable output,
        excluding any fields with a None value.
        """
        return self.model_dump(
            mode='json',
            exclude_none=True
        )