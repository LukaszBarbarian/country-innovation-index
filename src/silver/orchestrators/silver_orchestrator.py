
import logging
from src.silver.contexts.layer_runtime_context import LayerRuntimeContext
from src.silver.storage_manager.silver_storage_manager import SilverStorageManager
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.silver.mediators.model_builder_mediator import ModelBuilderMediator
from typing import List, Optional
import traceback 
import injector
from src.silver.models.model_module import ModelModule


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
            di_injector = injector.Injector([ModelModule()])
            model_builder_mediator = ModelBuilderMediator(di_injector=di_injector, context=context)

            mediator_result = await model_builder_mediator.run()








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
            logger.error(f"Error in SilverOrchestrator for {context.dataset_name}: {e}")
            
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