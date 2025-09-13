# src/silver/results/silver_result_builder.py
from typing import List
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.models.base_context import ContextBase
from src.common.models.process_model_result import ProcessModelResult


class SilverResultBuilder:
    """
    A builder class for creating a standardized `OrchestratorResult` object
    at the conclusion of the Silver ETL layer's execution.

    It provides methods to generate both success and failure results,
    populating the object with key metrics like status, duration, and output paths.
    """
    def __init__(self, context: ContextBase):
        """
        Initializes the builder with the ETL run context.
        """
        self._context = context

    def create_final_orchestrator_result(
        self,
        persisted_models: List[ProcessModelResult],
        summary_url: str,
        duration_ms: int
    ) -> OrchestratorResult:
        """
        Creates the final orchestration result based on the processed models.

        Args:
            persisted_models (List[ProcessModelResult]): A list of results for each
                                                         successfully processed model.
            summary_url (str): The URL where the final summary file is stored.
            duration_ms (int): The total duration of the orchestration in milliseconds.

        Returns:
            OrchestratorResult: The final result object with a status of 'COMPLETED' or 'NO_RESULTS'.
        """
        status = "COMPLETED" if persisted_models else "NO_RESULTS"
        message = "Silver orchestration finished successfully." if persisted_models else "Silver orchestration finished with no models."
        
        return OrchestratorResult(
            status=status,
            correlation_id=self._context.correlation_id,
            etl_layer=self._context.etl_layer,
            env=self._context.env,
            message=message,
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
        """
        Creates an orchestration result for a critical failure.

        Args:
            error (Exception): The exception that caused the failure.
            duration_ms (int): The duration of the execution before failure.

        Returns:
            OrchestratorResult: The result object with a 'FAILED' status and error details.
        """
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