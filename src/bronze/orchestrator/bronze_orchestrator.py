# src/bronze/orchestrator/bronze_orchestrator.py
import asyncio
from typing import List

from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.models.manifest import BronzeManifestSource
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.ingestion_strategy_factory import IngestionStrategyFactory
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry
from src.common.models.base_process_result import BaseProcessResult

@OrchestratorRegistry.register(ETLLayer.BRONZE)
class BronzeOrchestrator(BaseOrchestrator):
    """
    An orchestrator specifically designed for the Bronze layer of an ETL pipeline.

    This class orchestrates data ingestion for all sources defined in the manifest.
    It identifies the appropriate ingestion strategy for each source and executes
    the ingestion tasks concurrently. It is registered to handle processes
    within the `ETLLayer.BRONZE`.
    """
    async def run(self, context: BronzeContext) -> List[BaseProcessResult]:
        """
        Executes the data ingestion process for all sources defined in the context's manifest.

        This method iterates through each source in the manifest, finds the correct
        ingestion strategy using `IngestionStrategyFactory`, and creates a list
        of asynchronous tasks. It then runs these tasks concurrently to
        ingest data from all sources in parallel.

        Args:
            context (BronzeContext): The context object containing the manifest
                                     with the list of sources to be processed.

        Returns:
            List[BaseProcessResult]: A list of results from each ingestion task,
                                     which can be either successful or contain exceptions.
        """
        tasks = []
        for source in context.manifest.sources:
            strategy = IngestionStrategyFactory.get_instance(
                source.source_config_payload.domain_source_type,
                config=self.config,
                context=context
            )
            tasks.append(strategy.ingest(source))

        processed_results = await asyncio.gather(*tasks, return_exceptions=True)
        return processed_results