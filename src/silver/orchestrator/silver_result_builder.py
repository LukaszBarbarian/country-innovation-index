# src/silver/results/silver_result_builder.py
import datetime
from typing import List, Dict, Optional
from src.common.models.etl_model import EtlModel
from src.common.models.etl_model_result import EtlModelResult
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.models.base_context import BaseContext


class SilverResultBuilder:
    def __init__(self, context: BaseContext):
        self._context = context

    def create_etl_model_result(self, model: EtlModel, status: str, output_path: Optional[str] = "N/A", error: Optional[Exception] = None) -> EtlModelResult:
        """Tworzy pojedynczy wynik dla przetworzonego modelu."""
        error_details = {"error_message": str(error)} if error else {}
        model_name = model.type.value if model else "N/A"
        operation_type = "PERSISTED" if status == "COMPLETED" else "FAILED"
        
        return EtlModelResult(
            model_name=model_name,
            status=status,
            output_path=output_path,
            correlation_id=self._context.correlation_id,
            timestamp=datetime.datetime.utcnow().isoformat(),
            operation_type=operation_type,
            error_details=error_details
        )
        
    def create_final_orchestrator_result(self, etl_model_results: List[EtlModelResult], summary_url: str, duration_ms: int) -> OrchestratorResult:
        """Tworzy ostateczny wynik orkiestracji na podstawie wyników modeli."""
        overall_status = self._get_overall_status(etl_model_results)
        
        return OrchestratorResult(
            status=overall_status,
            correlation_id=self._context.correlation_id,
            etl_layer=self._context.etl_layer,
            env=self._context.env,
            message=f"Silver orchestration {overall_status}. See summary for details.",
            output_paths=[r.output_path for r in etl_model_results if r.output_path and r.output_path != "N/A"],
            processed_items=len(etl_model_results),
            duration_in_ms=duration_ms,
            summary_url=summary_url
        )
    
    def create_error_orchestrator_result(self, error: Exception, duration_ms: int) -> OrchestratorResult:
        """Tworzy wynik orkiestracji w przypadku błędu na wyższym poziomie."""
        return OrchestratorResult(
            status="FAILED",
            correlation_id=self._context.correlation_id,
            etl_layer=self._context.etl_layer,
            env=self._context.env,
            message=f"Silver orchestration failed due to a fatal error: {error}",
            output_paths=[],
            summary_url=None,
            processed_items=0,
            duration_in_ms=duration_ms,
            error_details={"error_message": str(error)}
        )

    def _get_overall_status(self, etl_model_results: List[EtlModelResult]) -> str:
        """Określa ogólny status na podstawie wyników modeli."""
        if not etl_model_results:
            return "NO_RESULTS"
        
        if any(r.status == "FAILED" for r in etl_model_results):
            return "FAILED"
        
        if any(r.status == "SKIPPED" for r in etl_model_results):
            return "PARTIAL_SUCCESS"
            
        return "COMPLETED"