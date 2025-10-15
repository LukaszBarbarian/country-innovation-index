# src/silver/orchestrator/silver_orchestrator.py
import asyncio
import datetime
from typing import List, Any
import injector

from src.common.enums.etl_layers import ETLLayer
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry
from src.common.models.base_process_result import BaseProcessResult
from src.gold.contexts.gold_layer_context import GoldContext
from src.gold.di.gold_module import GoldModule
from src.gold.models.models import GoldManifest
from src.gold.models.model_director import ModelDirector
from src.silver.context.silver_context import SilverContext
from src.common.model_persister.model_persister import ModelPersister


@OrchestratorRegistry.register(ETLLayer.GOLD)
class GoldOrchestrator(BaseOrchestrator):
    """
    An orchestrator for the Gold ETL layer.

    This class is responsible for managing the end-to-end process of building and
    persisting analytical models. It uses dependency injection to get necessary
    components and leverages asyncio to run model-building tasks concurrently.
    """
    async def run(self, context: GoldContext) -> List[BaseProcessResult]:
        """
        Executes the Gold layer ETL process.

        It initializes the dependency injector, creates tasks for each model
        defined in the manifest, and runs them concurrently.

        Args:
            context (GoldContext): The context object containing manifest and other run details.

        Returns:
            List[BaseProcessResult]: A list of results for each processed model.
        """
        di_injector = self.init_di(context, self.spark, self.config)
        model_director = di_injector.get(ModelDirector)
        persister = ModelPersister(layer=ETLLayer.GOLD, config=self.config, context=context, spark=self.spark)

        tasks = [
            self._process_single_model(model, model_director, persister, context)
            for model in context.manifest.models
        ]
        
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_single_model(self, 
                                    model: GoldManifest, 
                                    model_director: ModelDirector, 
                                    persister: ModelPersister, 
                                    context: GoldContext) -> BaseProcessResult:
        """
        Processes a single analytical model.

        This private method handles the building and persistence of an individual
        model, including time tracking and error handling. It returns a result object
        that summarizes the outcome of the process.

        Args:
            model (GoldManifest): The manifest data for the model to be processed.
            model_director (ModelDirector): The director responsible for building the model.
            persister (ModelPersister): The persister responsible for saving the model.
            context (GoldContext): The current ETL context.

        Returns:
            BaseProcessResult: An object representing the result of the process,
                               including success status, duration, and any errors.
        """
        model_start_time = datetime.datetime.now(datetime.timezone.utc)
        try:
            built_model = await model_director.get_built_model(model)
            persisted_result = persister.persist_model(built_model)
            model_duration_ms = int((datetime.datetime.now(datetime.timezone.utc) - model_start_time).total_seconds() * 1000)
            persisted_result.duration_in_ms = model_duration_ms
            return persisted_result
        except Exception as e:
            model_duration_ms = int((datetime.datetime.now(datetime.timezone.utc) - model_start_time).total_seconds() * 1000)
            # Return a result object with an error so that the OrchestratorResultBuilder can handle it.
            return BaseProcessResult(
                status="FAILED",
                correlation_id=context.correlation_id,
                duration_in_ms=model_duration_ms,
                error_details={"error_message": str(e)}
            )

    def init_di(self, context, spark, config) -> injector.Injector:
        """
        Initializes the dependency injector for the Gold layer.

        Args:
            context: The ETL context.
            spark: The Spark service instance.
            config: The configuration manager instance.

        Returns:
            injector.Injector: The configured injector instance.
        """
        return injector.Injector([GoldModule(context, spark, config)])