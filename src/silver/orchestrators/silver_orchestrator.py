
import logging
from src.common.contexts.layer_context import LayerContext
from src.common.enums.etl_layers import ETLLayer
from src.common.registers.orchestrator_registry import OrchestratorRegistry
from src.common.storage_account.blob_storage_manager import BlobStorageManager
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.enums.model_type import ModelType
from typing import List, Optional
import traceback 
from injector import Injector
from src.silver.di.silver_module import SilverModule
from src.common.factories.model_builder_factory import ModelBuilderFactory
from src.common.models.base_model import BaseModel
from pyspark.sql import SparkSession



logger = logging.getLogger(__name__)


@OrchestratorRegistry.register(ETLLayer.BRONZE)
class SilverOrchestrator(BaseOrchestrator):
    def __init__(self, config):
        super().__init__(config)
         
    async def run(self, context: LayerContext) -> OrchestratorResult:
        logger.info(f"Starting transformation CorrelationId: {context.correlation_id}")
        storage_manager = BlobStorageManager(context.etl_layer.value)

        final_output_path: Optional[str] = None 

        try:
            if not self.spark:
                raise

            di_injector = self.init_di(context, self.spark)

            for model_type in ModelType:
                builder_class = ModelBuilderFactory.get_class(model_type)
                model_builder = di_injector.get(builder_class)
                model = await model_builder.run()
                
                
            return OrchestratorResult(
                status="COMPLETED",
                correlation_id=context.correlation_id,
                queue_message_id=context.queue_message_id,
                layer_name=context.etl_layer,
                env=context.env,
                message="No records fetched, skipping file save.",
                output_paths=None,
            )


        except Exception as e:
            return self._create_final_result_for_error(context, e)


    

    def init_di(self, context: LayerContext, spark: SparkSession) -> Injector:
        return Injector(SilverModule(context, spark))
        
        