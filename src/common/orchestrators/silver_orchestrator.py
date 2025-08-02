import logging
from pyspark.sql import SparkSession, DataFrame 
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.enums.model_type import ModelType
from src.common.factories.model_builder_factory import ModelBuilderFactory 
from src.common.builders.base_model_builder import BaseModelBuilder 
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.contexts.layer_runtime_context import LayerRuntimeContext 
from src.common.contexts.silver_context import SilverContext
from src.common.factories.data_reader_factory import DataReaderFactory # Nadal importujemy, bo jej używamy do budowania
from typing import Any, Dict 
from src.common.storage_account.silver_storage_manager import SilverStorageManager
import traceback


logger = logging.getLogger(__name__)

class SilverOrchestrator(BaseOrchestrator):
    """
    Uproszczony Orkiestrator warstwy Silver.
    Zarządza procesem budowania pojedynczego modelu danych.
    """
    # SparkSession nadal nie jest w konstruktorze.
    # DataReaderFactory będzie budowana W ŚRODKU.
    def __init__(self, config: Any): # Usunięto data_reader_factory z parametrów konstruktora
        super().__init__(config)

        self.data_reader_factory = DataReaderFactory(config=config)
        
        self.storage_manager = SilverStorageManager()

    async def run(self, runtime_context: LayerRuntimeContext[SilverContext]) -> OrchestratorResult:
        """
        Uruchamia proces orkiestracji warstwy Silver dla pojedynczego modelu.
        Przyjmuje gotowy LayerRuntimeContext z zewnątrz.
        Oczekuje, że runtime_context.processing_config_payload będzie zawierał "model_to_build" jako ModelType.
        Zwraca OrchestratorResult z sukcesem lub porażką.
        """

        correlation_id = runtime_context.correlation_id 
        queue_message_id = runtime_context.queue_message_id
        
        model_build_instruction = runtime_context.processing_config_payload.get("model_to_build")
        
        if not model_build_instruction:
            error_msg = "Payload in runtime_context must specify 'model_to_build' for this simplified orchestrator."
            logger.error(error_msg)
            return OrchestratorResult(
                status="FAILED",
                correlation_id=correlation_id,
                queue_message_id=queue_message_id,
                message=error_msg,
                error_details={"errorMessage": error_msg}
            )

        logger.info(f"Starting Silver orchestration for Correlation ID: {correlation_id}. Building model: {model_build_instruction}")

        try:
            model_type = ModelType[model_build_instruction.upper()] 
            
            builder_instance: BaseModelBuilder = ModelBuilderFactory.get_instance(model_type)
            
            final_df: DataFrame = builder_instance.run(runtime_context) 

            logger.info(f"Model '{model_type.name}' built successfully. Rows: {final_df.count()}. (Not saving to storage in this simplified version)")

            return OrchestratorResult(
                status="COMPLETED",
                correlation_id=correlation_id,
                queue_message_id=queue_message_id,
                api_name="N/A", 
                dataset_name=model_type.name, 
                layer_name="Silver", 
                message=f"Silver orchestration completed successfully for model: {model_type.name}",
                output_path=None, 
                api_response_status_code=None
            )

        except KeyError:
            error_msg = f"Unknown model type specified: {model_build_instruction}. Make sure it exists in ModelType enum."
            logger.error(error_msg)
            return OrchestratorResult(
                status="FAILED",
                correlation_id=correlation_id,
                queue_message_id=queue_message_id,
                message=error_msg,
                error_details={"errorMessage": error_msg}
            )
        except ValueError as e: 
            logger.error(f"Error building model {model_type.name}: {e}", exc_info=True)
            return OrchestratorResult(
                status="FAILED",
                correlation_id=correlation_id,
                queue_message_id=queue_message_id,
                message=f"Error building model {model_type.name}: {e}",
                error_details={"errorType": type(e).__name__, "errorMessage": str(e), "stackTrace": traceback.format_exc()}
            )
        except Exception as e:
            logger.error(f"Unexpected error in SilverOrchestrator for Correlation ID {correlation_id}: {e}", exc_info=True)
            
            error_details = {
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc()
            }
            
            return OrchestratorResult(
                status="FAILED",
                correlation_id=correlation_id,
                queue_message_id=queue_message_id,
                api_name="N/A",
                dataset_name="SilverModels", 
                layer_name="Silver", 
                message=f"Silver orchestration failed for Correlation ID {correlation_id}: {str(e)}",
                output_path=None, 
                api_response_status_code=None,
                error_details=error_details
            )