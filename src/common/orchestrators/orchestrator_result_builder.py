from dataclasses import asdict
from typing import List, Any
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.models.base_context import ContextBase
from src.common.models.base_process_result import BaseProcessResult
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.config.config_manager import ConfigManager
from src.common.azure_clients.blob_client_manager import BlobClientManager
import datetime
import logging

logger = logging.getLogger(__name__)

class OrchestratorResultBuilder:
    """
    A builder class for creating and managing `OrchestratorResult` objects.

    This class is responsible for aggregating results from multiple pipeline tasks,
    calculating the overall status and duration, and saving a summary file of the
    orchestration process to a data lake.
    """
    def __init__(self, context: ContextBase, config: ConfigManager):
        """
        Initializes the OrchestratorResultBuilder with the pipeline context and configuration.

        Args:
            context (ContextBase): The context object for the current pipeline run.
            config (ConfigManager): The configuration manager instance.
        """
        self._context = context
        self._config = config
        self._results: List[BaseProcessResult] = []
        self._blob_client_manager = BlobClientManager(container_name=context.etl_layer.value.lower())

    def add_result(self, result: BaseProcessResult):
        """
        Adds a processing result to the internal list.

        Args:
            result (BaseProcessResult): The result object from a single pipeline task.
        """
        self._results.append(result)

    async def build_and_save(self) -> OrchestratorResult:
        """
        Builds the final `OrchestratorResult` object and saves the summary file.

        This asynchronous method orchestrates the final steps of the pipeline run.
        It calculates the total duration, determines the overall status, and then
        constructs the final result object. It also calls the method to save a
        summary file to the data lake and updates the result object with the file's URL.

        Returns:
            OrchestratorResult: The complete and finalized orchestrator result.
        """
        start_time = datetime.datetime.now(datetime.timezone.utc)

        try:
            final_result = self.create_final_orchestrator_result()
            
            summary_url = await self.save_summary_file(final_result.duration_in_ms)
            final_result.summary_url = summary_url

            return final_result
        except Exception as e:
            duration_ms = int((datetime.datetime.now(datetime.timezone.utc) - start_time).total_seconds() * 1000)
            logger.exception(f"Orchestrator result builder failed to finalize results for {self._context.etl_layer.value}")
            return self.create_error_orchestrator_result(e, duration_ms)

    def create_final_orchestrator_result(self) -> OrchestratorResult:
        """
        Creates a final `OrchestratorResult` object based on the aggregated results.

        This method compiles all collected results to determine the overall status,
        total duration, and other key metrics for the entire orchestration process.

        Returns:
            OrchestratorResult: The comprehensive result object for the orchestration.
        """
        overall_status = self.get_overall_status()
        duration_ms = sum(r.duration_in_ms for r in self._results)
        
        return OrchestratorResult(
            status=overall_status,
            correlation_id=self._context.correlation_id,
            etl_layer=self._context.etl_layer,
            env=self._context.env,
            processed_items=len(self._results),
            duration_in_ms=duration_ms,
            results=[asdict(r) for r in self._results],
            message=f"{self._context.etl_layer.value} orchestration {overall_status}. See summary for details."
        )

    def create_error_orchestrator_result(self, error: Exception, duration_ms: int) -> OrchestratorResult:
        """
        Creates an `OrchestratorResult` object to represent a failed orchestration.

        This method is a helper for generating a result when a critical error occurs
        in the orchestration process itself, before individual task results can be collected.

        Args:
            error (Exception): The exception object that caused the failure.
            duration_ms (int): The total duration of the process before failure, in milliseconds.

        Returns:
            OrchestratorResult: The result object with a "FAILED" status and error details.
        """
        return OrchestratorResult(
            status="FAILED",
            correlation_id=self._context.correlation_id,
            etl_layer=self._context.etl_layer,
            env=self._context.env,
            message=f"Orchestration failed: {str(error)}",
            processed_items=0,
            duration_in_ms=duration_ms,
            error_details={"error_message": str(error)}
        )

    def get_overall_status(self) -> str:
        """
        Determines the overall status of the orchestration based on individual task statuses.

        The logic follows a hierarchy:
        - If any task failed, the overall status is "FAILED".
        - If no tasks failed but at least one was skipped, the status is "PARTIAL_SUCCESS".
        - If all tasks were completed successfully, the status is "COMPLETED".
        - If there were no results at all, the status is "NO_RESULTS".

        Returns:
            str: The determined overall status string.
        """
        if not self._results:
            return "NO_RESULTS"
        if any(r.status == "FAILED" for r in self._results):
            return "FAILED"
        if any(r.status == "SKIPPED" for r in self._results):
            return "PARTIAL_SUCCESS"
        return "COMPLETED"

    async def save_summary_file(self, duration_ms: int) -> str:
        """
        Saves a summary file of the orchestration results to the data lake.

        This method uses a `StorageFileBuilder` to format the summary data and then
        uploads it to Azure Blob Storage using the `BlobClientManager`. The file
        serves as a comprehensive audit log of the orchestration run.

        Args:
            duration_ms (int): The total duration of the orchestration in milliseconds.

        Returns:
            str: The full URL path to the saved summary file.
        """
        file_builder = StorageFileBuilderFactory.get_instance(self._context.etl_layer, config=self._config)
        storage_account_name = self._config.get("DATA_LAKE_STORAGE_ACCOUNT_NAME")
        
        summary_output = file_builder.build_summary_file_output(
            context=self._context,
            results=self._results,
            container_name=self._context.etl_layer.value.lower(),
            storage_account_name=storage_account_name,
            duration_orchestrator=duration_ms
        )
        
        summary_content_bytes = summary_output["file_content_bytes"]
        summary_file_info = summary_output["file_info"]

        await self._blob_client_manager.upload_blob(
            file_content_bytes=summary_content_bytes,
            file_info=summary_file_info,
            overwrite=True
        )
        
        return summary_file_info.full_blob_url