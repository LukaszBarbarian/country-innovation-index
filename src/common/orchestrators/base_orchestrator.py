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
        self.spark = spark
        

    @abstractmethod
    async def run(self, context: BaseContext) -> OrchestratorResult:
        pass
    







    

    def _create_final_result_for_error(self, 
                                     context: BaseContext, 
                                     e: Exception,
                                     duration_in_ms: Optional[int] = None) -> OrchestratorResult:
        """
        Tworzy końcowy wynik orkiestracji w przypadku nieobsłużonego błędu.
        """
        status = "FAILED"
        message = f"Orchestration failed with error: {str(e)}"
        
        return OrchestratorResult(
            status=status,
            correlation_id=context.correlation_id,
            etl_layer=context.etl_layer,
            env=context.env,
            message=message,
            output_paths=None,
            processed_items=0,
            duration_in_ms=duration_in_ms, # Dodajemy czas trwania
            error_details={
                "errorType": type(e).__name__,
                "errorMessage": str(e)
            }
        )