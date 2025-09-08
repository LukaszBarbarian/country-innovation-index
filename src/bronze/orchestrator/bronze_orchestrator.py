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
    async def run(self, context: BronzeContext) -> List[BaseProcessResult]:
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