import asyncio
import datetime
import logging

import injector
from src.common.azure_clients.blob_client_manager import BlobClientManager
from src.common.config.config_manager import ConfigManager
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.common.models.base_context import BaseContext
from src.common.enums.etl_layers import ETLLayer
from src.common.models.etl_model import EtlModel
from src.common.models.etl_model_result import EtlModelResult
from src.common.models.file_info import FileInfo
from src.common.models.manifest import Model
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry
from src.common.models.orchestrator_result import OrchestratorResult
from typing import Any, List, cast
from src.common.spark.spark_service import SparkService
from src.silver.builders.model_director import ModelDirector
from src.silver.context.silver_context import SilverLayerContext
from src.silver.di.silver_module import SilverModule
from src.silver.model_persister.model_persister import ModelPersister
from src.silver.orchestrator.silver_result_builder import SilverResultBuilder


logger = logging.getLogger(__name__)


@OrchestratorRegistry.register(ETLLayer.SILVER)
class SilverOrchestrator(BaseOrchestrator):
    def __init__(self, config: ConfigManager, spark: SparkService):
        super().__init__(config, spark)
        self.blob_client_manager = BlobClientManager(container_name=ETLLayer.SILVER.value)

    async def run(self, context: SilverLayerContext) -> OrchestratorResult:
        logger.info(f"Starting Silver process. Correlation ID: {context.correlation_id}")
        
        etl_model_results: List[EtlModelResult] = []
        start_time_orchestrator = datetime.datetime.utcnow()
        summary_url: str = None
        
        try: 
            result_builder = SilverResultBuilder(context)
            di_injector = self.init_di(context, self.spark, self.config)
            model_director = di_injector.get(ModelDirector)
            persister = ModelPersister(config=self.config, context=context, spark=self.spark)

            tasks = []
            for model_config in context.models:
                tasks.append(self._build_and_persist_model(
                    model_director, persister, model_config, context
                ))

            # Uruchamiamy wszystkie zadania jednocześnie.
            etl_model_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Obsługa wyjątków i logowanie
            final_results = []
            for result in etl_model_results:
                if isinstance(result, Exception):
                    logger.error(f"Failed to build a model: {result.model.type.name}")
                    # Tworzymy wynik błędu, bez pełnego obiektu modelu
                    error_result = result_builder.create_etl_model_result(
                        model=None, status="FAILED", error=error_result)
                    etl_model_results.append(error_result)
                    continue
                else:
                    final_results.append(result)

            end_time_orchestrator = datetime.datetime.utcnow()
            duration_in_ms_orchestrator = int((end_time_orchestrator - start_time_orchestrator).total_seconds() * 1000)
            
            summary_url = await self._save_summary_file(context, final_results, duration_in_ms_orchestrator)
            return result_builder.create_final_orchestrator_result(final_results, summary_url, duration_in_ms_orchestrator)

        except Exception as e:
            end_time_orchestrator = datetime.datetime.utcnow()
            duration_in_ms_orchestrator = int((end_time_orchestrator - start_time_orchestrator).total_seconds() * 1000)
            return result_builder.create_error_orchestrator_result(e, duration_in_ms_orchestrator)
    
    async def _build_and_persist_model(self, 
                                       model_director: ModelDirector, 
                                       persister: ModelPersister, 
                                       model_config: Model, 
                                       context: SilverLayerContext) -> EtlModelResult:
        """
        Pomocnicza coroutine, która mierzy czas budowania i persystencji modelu.
        """
        start_time_model = datetime.datetime.utcnow()
        try:
            built_model = await model_director.get_built_model(model_config.model_name)
            model: EtlModel = cast(EtlModel, built_model)
            model_result = persister.persist_model(model)
            
            end_time_model = datetime.datetime.utcnow()
            duration_in_ms = int((end_time_model - start_time_model).total_seconds() * 1000)
            
            final_result = EtlModelResult(
                model=model,
                status="COMPLETED",
                output_path=model_result.output_path,
                correlation_id=context.correlation_id,
                timestamp=datetime.datetime.utcnow().isoformat(),
                operation_type="UPSERT",
                record_count=model_result.record_count,
                duration_in_ms=duration_in_ms
            )
            return final_result
        except Exception as e:
            end_time_model = datetime.datetime.utcnow()
            duration_in_ms = int((end_time_model - start_time_model).total_seconds() * 1000)
            
            error_result = EtlModelResult(
                model=model_config,
                status="FAILED",
                output_path=None,
                correlation_id=context.correlation_id,
                timestamp=datetime.datetime.utcnow().isoformat(),
                operation_type="UPSERT",
                error_details={"error": str(e)},
                duration_in_ms=duration_in_ms
            )
            return error_result


            
    async def _save_summary_file(self, context: BaseContext, etl_model_results: List[EtlModelResult], duration_in_ms_orchestrator: int ) -> str:
        """Saves the summary file to ADLS Gen2 using the same approach as Bronze orchestrator."""
        file_builder = StorageFileBuilderFactory.get_instance(ETLLayer.SILVER, config=self.config)
        storage_account_name = self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")

        
        summary_output = file_builder.build_summary_file_output(
            context=context,
            results=etl_model_results,
            container_name=context.etl_layer.value.lower(),
            storage_account_name=storage_account_name,
            duration_orchestrator = duration_in_ms_orchestrator
        )
        
        summary_content_bytes = summary_output["file_content_bytes"]
        summary_file_info: FileInfo = summary_output["file_info"]
        
        await self.blob_client_manager.upload_blob(
            file_content_bytes=summary_content_bytes,
            file_info=summary_file_info
        )
        
        return summary_file_info.full_blob_url
    
        

    def init_di(self, context: BaseContext, spark: SparkService, config: ConfigManager) -> 'injector.Injector':
        return injector.Injector([SilverModule(context, spark, config)])
    
