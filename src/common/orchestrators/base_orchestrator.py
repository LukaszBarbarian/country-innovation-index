# common/orchestrators/base_orchestrator.py
from abc import ABC, abstractmethod
from dataclasses import field
from typing import Any, Dict, List, Optional, overload
from src.common.contexts.layer_context import LayerContext
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.config.config_manager import ConfigManager


class BaseOrchestrator(ABC):
    def __init__(self, config: ConfigManager):
        self.config = config
        self.storage_account_name = self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")
        

    @abstractmethod
    async def run(self, context: LayerContext) -> OrchestratorResult:
        pass
    

        
    def create_result(self, 
                      context: LayerContext,
                      status: str,
                      message: str, 
                      source_response_status_code: Optional[int] = None,
                      all_output_paths: Optional[List[str]] = None, 
                      error_details: Optional[Dict[str, Any]] = field(default_factory=dict) ) -> OrchestratorResult:
        
        return OrchestratorResult(
                status=status,
                correlation_id=context.correlation_id,
                queue_message_id=context.queue_message_id,
                domain_source=context.domain_source,
                domain_source_type=context.domain_source_type,
                dataset_name=context.dataset_name,
                layer_name=context.etl_layer,
                env=context.env,
                message=message,
                output_paths=all_output_paths,
                source_response_status_code=source_response_status_code,
                error_details=error_details
            )