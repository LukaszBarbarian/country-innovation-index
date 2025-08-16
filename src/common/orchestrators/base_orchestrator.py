# common/orchestrators/base_orchestrator.py
from abc import ABC, abstractmethod
from dataclasses import field
from typing import Any, Dict, List, Optional, overload
from src.common.models.base_context import BaseContext
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.config.config_manager import ConfigManager
from src.common.spark.spark_service import SparkService


class BaseOrchestrator(ABC):
    def __init__(self, config: ConfigManager, spark: Optional[SparkService] = None):
        self.config = config
        self.storage_account_name = self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")
        self.spark = spark
        

    @abstractmethod
    async def run(self, context: BaseContext) -> OrchestratorResult:
        pass
    







    

    def _create_final_result_for_error(self, context: BaseContext, error: Exception) -> OrchestratorResult:
        """
        Metoda pomocnicza do tworzenia wyniku w przypadku nieoczekiwanego błędu.
        """
        return OrchestratorResult(
            status="FAILED",
            correlation_id=context.correlation_id,
            layer_name=context.etl_layer,
            env=context.env,
            message=f"Orchestrator failed due to an internal error: {str(error)}",
            error_details={
                "errorType": type(error).__name__,
                "errorMessage": str(error)
            }
        )    