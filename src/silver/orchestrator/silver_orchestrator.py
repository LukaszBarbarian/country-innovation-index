
import logging
from logging import config

import injector
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import BaseContext
from src.common.enums.etl_layers import ETLLayer
from src.common.models.etl_model import EtlModel
from src.common.registers.orchestrator_registry import OrchestratorRegistry
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.enums.model_type import ModelType
from typing import List, Optional
from injector import Injector
from src.common.spark.spark_service import SparkService
from src.silver.di.silver_module import SilverModule
from src.common.factories.model_builder_factory import ModelBuilderFactory
from src.silver.model_persister.model_persister import ModelPersister


logger = logging.getLogger(__name__)


@OrchestratorRegistry.register(ETLLayer.SILVER)
class SilverOrchestrator(BaseOrchestrator):
         
    async def run(self, context: BaseContext) -> OrchestratorResult:
        logger.info(f"Starting transformation CorrelationId: {context.correlation_id}")

        final_output_path: List[str] = None 

        try:
            if not self.spark:
                raise

            di_injector = self.init_di(context, self.spark, self.config)

            for model_type in ModelType:
                builder_class = ModelBuilderFactory.get_class(model_type)
                model_builder = di_injector.get(builder_class)
                model_builder.set_identity(model_type)
                etl_model: EtlModel = await model_builder.run()

                persister = ModelPersister(config=self.config, context=context, spark=self.spark)
                path = persister.persist_model(etl_model)
                final_output_path.append(path)
                
                
            return OrchestratorResult(
                status="COMPLETED",
                correlation_id=context.correlation_id,
                layer_name=context.etl_layer,
                env=context.env,
                message="Silver process completed.",
                output_paths=final_output_path,
            )


        except Exception as e:
            return self._create_final_result_for_error(context, e)


    

    def init_di(self, context: BaseContext, spark: SparkService, config: ConfigManager) -> Injector:
        return Injector(SilverModule(context, spark, config))
        
        