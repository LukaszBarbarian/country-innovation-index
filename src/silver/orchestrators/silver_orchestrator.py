
import logging
from src.silver.contexts.layer_runtime_context import LayerRuntimeContext
from src.silver.contexts.silver_context import SilverContext
from src.silver.storage_manager.silver_storage_manager import SilverStorageManager
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.enums.model_type import ModelType
from typing import List, Optional
import traceback 
from injector import Injector
from src.silver.di.silver_module import SilverModule
from src.common.factories.model_builder_factory import ModelBuilderFactory
from src.common.models.base_model import BaseModel

import src.silver.init.silver_init 

logger = logging.getLogger(__name__)

class SilverOrchestrator(BaseOrchestrator):
    def __init__(self, config):
        super().__init__(config)
         
        self.storage_manager = SilverStorageManager()

    async def run(self, context: LayerRuntimeContext) -> OrchestratorResult:
        logger.info(f"Starting transformation CorrelationId: {context.correlation_id}")

        final_output_path: Optional[str] = None 

        try:
            di_injector = self.init_di(context)

            for model_type in ModelType:
                builder_class = ModelBuilderFactory.get_class(model_type)
                model_builder = di_injector.get(builder_class)
                model = await model_builder.run()
                
                
            print(model)

            return OrchestratorResult(
                status="COMPLETED",
                correlation_id=context.correlation_id,
                queue_message_id=context.queue_message_id,
                layer_name=context.etl_layer.value,
                env=context.env,
                message="No records fetched, skipping file save.",
                output_path=None,
            )


        except Exception as e:
            logger.error(f"Error in SilverOrchestrator for {context.correlation_id}: {e}")
            
            error_details = {
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc()
            }
            
            return OrchestratorResult(
                status="FAILED",
                correlation_id=context.correlation_id,
                queue_message_id=context.queue_message_id,
                layer_name=context.etl_layer.value, 
                env=context.env,
                message=f"Silver orchestration failed: {str(e)}",
                output_path=final_output_path,
                error_details=error_details
            )
        

    

    def init_di(self, context: LayerRuntimeContext) -> Injector:
        return Injector(SilverModule(context.layer_context, context.spark))
