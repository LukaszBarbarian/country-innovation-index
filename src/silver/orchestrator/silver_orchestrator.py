import asyncio
import datetime
from typing import List, Any
import injector

from src.common.enums.etl_layers import ETLLayer
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry
from src.common.models.base_process_result import BaseProcessResult
from src.silver.builders.model_director import ModelDirector
from src.silver.context.silver_context import SilverContext
from src.silver.di.silver_module import SilverModule
from src.common.model_persister.model_persister import ModelPersister
from src.silver.models.models import SilverManifestModel


@OrchestratorRegistry.register(ETLLayer.SILVER)
class SilverOrchestrator(BaseOrchestrator):
    """
    The main orchestrator for the Silver ETL layer.

    This class coordinates the end-to-end process of building and persisting
    all analytical models defined in the Silver manifest. It leverages
    asynchronous processing to handle models in parallel and uses a
    dependency injection container to manage components.
    """
    async def run(self, context: SilverContext) -> List[BaseProcessResult]:
        """
        Executes the Silver ETL process.

        Args:
            context (SilverContext): The context object containing manifest and configuration.

        Returns:
            List[BaseProcessResult]: A list of results, one for each model processed,
            including success/failure status and metadata.
        """
        di_injector = self.init_di(context, self.spark, self.config)
        model_director = di_injector.get(ModelDirector)
        persister = ModelPersister(layer=ETLLayer.SILVER, config=self.config, context=context, spark=self.spark)

        tasks = [
            self._process_single_model(model, model_director, persister, context)
            for model in context.manifest.models
        ]
        
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_single_model(self, 
                                    model: SilverManifestModel, 
                                    model_director: ModelDirector, 
                                    persister: ModelPersister, 
                                    context: SilverContext) -> BaseProcessResult:
        """
        Processes a single model from the manifest.

        This method encapsulates the entire process for one model: building,
        persisting, and error handling.
        """
        model_start_time = datetime.datetime.utcnow()
        try:
            built_model = await model_director.get_built_model(model)
            persisted_result = persister.persist_model(built_model)
            model_duration_ms = int((datetime.datetime.utcnow() - model_start_time).total_seconds() * 1000)
            persisted_result.duration_in_ms = model_duration_ms
            return persisted_result
        except Exception as e:
            model_duration_ms = int((datetime.datetime.utcnow() - model_start_time).total_seconds() * 1000)
            # Return an error object so the OrchestratorResultBuilder can handle it.
            return BaseProcessResult(
                status="FAILED",
                correlation_id=context.correlation_id,
                duration_in_ms=model_duration_ms,
                error_details={"error_message": str(e)}
            )

    def init_di(self, context, spark, config) -> injector.Injector:
        """
        Initializes the dependency injection container for the Silver layer.
        """
        return injector.Injector([SilverModule(context, spark, config)])