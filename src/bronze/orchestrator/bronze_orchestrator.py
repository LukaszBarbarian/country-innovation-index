# src/bronze/orchestrators/bronze_orchestrator.py

import logging

from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.ingestion_strategy_factory import IngestionStrategyFactory
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.models.ingestion_result import IngestionResult
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry


logger = logging.getLogger(__name__)


@OrchestratorRegistry.register(ETLLayer.BRONZE)
class BronzeOrchestrator(BaseOrchestrator):
    def __init__(self, config: ConfigManager):
        super().__init__(config)
    """
    Główny orkiestrator dla warstwy Bronze. Koordynuje proces pozyskiwania danych
    przy użyciu odpowiedniej strategii (API, plik statyczny itd.).
    """
    async def run(self, context: BronzeLayerContext) -> OrchestratorResult:
        try:
            # 1. Wybór odpowiedniej strategii na podstawie typu źródła danych z kontekstu.
            ingestion_strategy = IngestionStrategyFactory.get_instance(
                context.domain_source_type, 
                config=self.config
            )
            
            logger.info(
                f"Running BronzeOrchestrator with strategy: {type(ingestion_strategy).__name__} "
                f"for dataset: {context.dataset_name}"
            )

            # 2. Uruchomienie wybranej strategii. Zwróci ona IngestionResult.
            ingestion_result: IngestionResult = await ingestion_strategy.ingest(context)

            # 3. Na podstawie wyniku strategii, budujemy ostateczny OrchestratorResult.
            return self._create_final_result(context, ingestion_result)

        except Exception as e:
            logger.error(f"Bronze orchestration failed with unhandled exception: {e}")
            return self._create_final_result_for_error(context, e)






    def _create_final_result(self, context: BronzeLayerContext, ingestion_result: IngestionResult) -> OrchestratorResult:
        """
        Metoda pomocnicza do tworzenia ostatecznego obiektu OrchestratorResult.
        """
        return OrchestratorResult(
            status=ingestion_result.status,
            correlation_id=context.correlation_id,
            layer_name=context.etl_layer,
            env=context.env,
            message=ingestion_result.message,
            domain_source=context.domain_source,
            domain_source_type=context.domain_source_type,
            dataset_name=context.dataset_name,
            output_paths=ingestion_result.output_paths,
            source_response_status_code=ingestion_result.source_response_status_code,
            error_details=ingestion_result.error_details
        )