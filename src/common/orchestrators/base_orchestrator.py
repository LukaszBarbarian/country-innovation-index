# common/orchestrators/base_orchestrator.py
from abc import ABC, abstractmethod
from dataclasses import field
from typing import Any, Dict, List, Optional, overload
from src.common.contexts.layer_context import LayerContext
from src.common.models.ingestion_result import IngestionResult
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.config.config_manager import ConfigManager


class BaseOrchestrator(ABC):
    def __init__(self, config: ConfigManager):
        self.config = config
        self.storage_account_name = self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")
        

    @abstractmethod
    async def run(self, context: LayerContext) -> OrchestratorResult:
        pass
    







    def _create_final_result(self, context: LayerContext, ingestion_result: IngestionResult) -> OrchestratorResult:
        """
        Metoda pomocnicza do tworzenia ostatecznego obiektu OrchestratorResult.
        """
        return OrchestratorResult(
            status=ingestion_result.status,
            correlation_id=context.correlation_id,
            queue_message_id=context.queue_message_id,
            layer_name=context.etl_layer,
            env=context.env,
            message=ingestion_result.message,
            domain_source=context.domain_source,
            domain_source_type=context.domain_source_type,
            dataset_name=context.dataset_name,
            output_paths=ingestion_result.output_paths,
            source_response_status_code=ingestion_result.source_response_status_code,
            error_details=ingestion_result.error_details
        )

    def _create_final_result_for_error(self, context: LayerContext, error: Exception) -> OrchestratorResult:
        """
        Metoda pomocnicza do tworzenia wyniku w przypadku nieoczekiwanego błędu.
        """
        return OrchestratorResult(
            status="FAILED",
            correlation_id=context.correlation_id,
            queue_message_id=context.queue_message_id,
            layer_name=context.etl_layer,
            env=context.env,
            message=f"Orchestrator failed due to an internal error: {str(error)}",
            domain_source=context.domain_source,
            domain_source_type=context.domain_source_type,
            dataset_name=context.dataset_name,
            error_details={
                "errorType": type(error).__name__,
                "errorMessage": str(error)
            }
        )    