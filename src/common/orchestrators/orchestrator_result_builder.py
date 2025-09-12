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
    def __init__(self, context: ContextBase, config: ConfigManager):
        self._context = context
        self._config = config
        self._results: List[BaseProcessResult] = []
        self._blob_client_manager = BlobClientManager(container_name=context.etl_layer.value.lower())

    def add_result(self, result: BaseProcessResult):
        """Dodaje wynik przetwarzania do listy."""
        self._results.append(result)

    async def build_and_save(self) -> OrchestratorResult:
        """Buduje finalny OrchestratorResult i zapisuje podsumowanie."""
        start_time = datetime.datetime.utcnow()

        try:
            # Tworzenie finalnego wyniku
            final_result = self.create_final_orchestrator_result()
            
            # Zapis podsumowania do Data Lake
            summary_url = await self.save_summary_file(final_result.duration_in_ms)
            final_result.summary_url = summary_url

            return final_result
        except Exception as e:
            duration_ms = int((datetime.datetime.utcnow() - start_time).total_seconds() * 1000)
            logger.exception(f"Orchestrator result builder failed to finalize results for {self._context.etl_layer.value}")
            return self.create_error_orchestrator_result(e, duration_ms)

    def create_final_orchestrator_result(self) -> OrchestratorResult:
        """Tworzy obiekt OrchestratorResult na podstawie zebranych wyników."""
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
        """Tworzy wynik orkiestracji w przypadku błędu."""
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
        """Określa ogólny status orkiestracji na podstawie statusów poszczególnych wyników."""
        if not self._results:
            return "NO_RESULTS"
        if any(r.status == "FAILED" for r in self._results):
            return "FAILED"
        if any(r.status == "SKIPPED" for r in self._results):
            return "PARTIAL_SUCCESS"
        return "COMPLETED"

    async def save_summary_file(self, duration_ms: int) -> str:
        """Zapisuje plik podsumowania do Data Lake."""
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
            overwrite=True        )
        
        return summary_file_info.full_blob_url