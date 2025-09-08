# src/silver/results/silver_result_builder.py
from typing import List
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.models.base_context import ContextBase
from src.common.models.process_model_result import ProcessModelResult


class SilverResultBuilder:
    def __init__(self, context: ContextBase):
        self._context = context

    def create_final_orchestrator_result(
        self,
        persisted_models: List[ProcessModelResult],
        summary_url: str,
        duration_ms: int
    ) -> OrchestratorResult:
        """Tworzy ostateczny wynik orkiestracji na podstawie przetworzonych modeli."""
        return OrchestratorResult(
            status="COMPLETED" if persisted_models else "NO_RESULTS",
            correlation_id=self._context.correlation_id,
            etl_layer=self._context.etl_layer,
            env=self._context.env,
            message="Silver orchestration finished successfully." if persisted_models else "Silver orchestration finished with no models.",
            output_paths=[
                model.output_path for model in persisted_models if model.output_path
            ],
            processed_items=len(persisted_models),
            duration_in_ms=duration_ms,
            summary_url=summary_url,
        )

    def create_error_orchestrator_result(
        self,
        error: Exception,
        duration_ms: int
    ) -> OrchestratorResult:
        """Tworzy wynik orkiestracji w przypadku błędu krytycznego."""
        return OrchestratorResult(
            status="FAILED",
            correlation_id=self._context.correlation_id,
            etl_layer=self._context.etl_layer,
            env=self._context.env,
            message=f"Silver orchestration failed: {error}",
            output_paths=[],
            processed_items=0,
            duration_in_ms=duration_ms,
            summary_url=None,
            error_details={"error_message": str(error)},
        )
