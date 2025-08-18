# src/bronze/orchestrators/bronze_orchestrator.py

import asyncio
import logging
import json
import datetime
from typing import List, Dict, Any, Union
import hashlib

from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.ingestion_strategy_factory import IngestionStrategyFactory
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.models.file_info import FileInfo
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.models.ingestions import IngestionContext, IngestionResult
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry
from src.common.azure_clients.blob_client_manager import BlobClientManager # Zakładamy, że masz tę klasę
from dataclasses import asdict

logger = logging.getLogger(__name__)


@OrchestratorRegistry.register(ETLLayer.BRONZE)
class BronzeOrchestrator(BaseOrchestrator):
    def __init__(self, config: ConfigManager):
        super().__init__(config)
        self.blob_client_manager = BlobClientManager(container_name=ETLLayer.BRONZE.value)

    async def run(self, context: BronzeLayerContext) -> OrchestratorResult:
        ingestion_results: List[IngestionResult] = []
        
        tasks = []
        for source in context.ingest_contexts:
            try:
                ingestion_strategy = IngestionStrategyFactory.get_instance(
                    source.source_config.domain_source_type, 
                    config=self.config
                )
                
                tasks.append(ingestion_strategy.ingest(source))
            except Exception as e:
                logger.error(f"Failed to prepare ingestion for source {source.source_config.dataset_name}: {e}")
                ingestion_results.append(
                    IngestionResult(
                        correlation_id=context.correlation_id,
                        env=context.env.value,
                        etl_layer=context.etl_layer.value,
                        domain_source=source.source_config.domain_source,
                        domain_source_type=source.source_config.domain_source_type,
                        dataset_name=source.source_config.dataset_name,
                        status="FAILED",
                        message=f"Orchestration failed to prepare for ingestion: {str(e)}"
                    )
                )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ingestion task failed with an exception: {result}")
                # Tutaj jest problematyczne, bo nie mamy kontekstu
                # Lepszym rozwiązaniem jest, aby strategie nie rzucały wyjątków,
                # tylko zwracały IngestionResult ze statusem FAILED
                ingestion_results.append(result)
            else:
                ingestion_results.append(result)

        total_duration_ms = sum(r.duration_in_ms for r in ingestion_results)

        
        summary_url = await self._save_ingestion_summary(context, ingestion_results)
        
        return self._create_final_result(context, ingestion_results, summary_url, total_duration_ms)



    def _create_error_ingestion_result(self, context: IngestionContext, error: Exception) -> IngestionResult:
        """Metoda pomocnicza do tworzenia IngestionResult w przypadku błędu na poziomie orkiestratora."""
        return IngestionResult(
            correlation_id=context.correlation_id,
            env=context.env.value,
            etl_layer=context.etl_layer.value,
            domain_source=context.source_config.domain_source,
            domain_source_type=context.source_config.domain_source_type,
            dataset_name=context.source_config.dataset_name,
            status="FAILED",
            message=f"Orchestration failed with error: {str(error)}",
            output_paths=None,
            error_details={"error_message": str(error)}
        )

    def _create_ingestion_summary_data(self, context: IngestionContext, ingestion_results: List[IngestionResult]) -> Dict[str, Any]:
        """Tworzy słownik z podsumowaniem do serializacji JSON."""
        # Używamy asdict do konwersji każdego obiektu dataclass na słownik
        ingestion_results_dicts = [asdict(result) for result in ingestion_results]

        return {
            "status": f"{context.etl_layer.name}_COMPLETED",
            "env": context.env.value,
            "etl_layer": context.etl_layer.value,
            "correlation_id": context.correlation_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "processed_items": len(ingestion_results),
            "results": ingestion_results_dicts
        }

    async def _save_ingestion_summary(self, context: IngestionContext, ingestion_results: List[IngestionResult]) -> str:
        file_builder = StorageFileBuilderFactory.get_instance(ETLLayer.BRONZE, config=self.config)

        # Używamy nowej, dedykowanej metody do budowania pliku podsumowania
        summary_output = file_builder.build_summary_file_output(
            context=context,
            ingestion_results=ingestion_results,
            container_name=ETLLayer.BRONZE.value
        )
        
        summary_content_bytes = summary_output["file_content_bytes"]
        summary_file_info: FileInfo = summary_output["file_info"]

        # Wywołanie upload_blob z obiektem FileInfo
        await self.blob_client_manager.upload_blob(
            file_content_bytes=summary_content_bytes,
            file_info=summary_file_info,
            overwrite=True,
            tags=summary_file_info.blob_tags
        )
        
        return summary_file_info.full_blob_url

    
    def _create_final_result(self, context: IngestionContext, ingestion_results: List[IngestionResult], summary_url: str, duration_ms: int) -> OrchestratorResult:
        """Tworzy uproszczony OrchestratorResult, zawierający link do podsumowania."""

        overall_status = self.get_overall_status(ingestion_results)
        return OrchestratorResult(
            status=overall_status,
            correlation_id=context.correlation_id,
            etl_layer=context.etl_layer,
            env=context.env,
            processed_items=1, # W tej implementacji zawsze 1, bo operujesz na pojedynczym IngestionResult
            duration_in_ms=duration_ms,
            summary_url=summary_url,
            message=f"Orchestration {overall_status}. See summary for details."
        )
    

    def get_overall_status(self, ingestion_results: List[IngestionResult]) -> str:
        """
        Ustalanie ogólnego statusu procesu na podstawie statusów poszczególnych wyników.
        """
        if not ingestion_results:
            return "NO_RESULTS"
            
        # Sprawdzenie, czy jakikolwiek wynik zakończył się niepowodzeniem
        if any(r.status == "FAILED" for r in ingestion_results):
            return "FAILED"
        
        # Sprawdzenie, czy są jakieś pominięte wyniki
        if any(r.status == "SKIPPED" for r in ingestion_results):
            return "PARTIAL_SUCCESS"
            
        # Jeśli nie ma błędów ani pominięć, oznacza to, że wszystko się udało
        return "COMPLETED"    