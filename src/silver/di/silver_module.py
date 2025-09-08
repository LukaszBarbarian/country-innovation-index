# src/silver/di/silver_module.py

from injector import Module, Binder, singleton, provider
from src.common.config.config_manager import ConfigManager
from src.common.di.di_module import DIModule
from src.common.di.builders_module import BuildersModule
from src.common.di.readers_module import ReadersModule
from src.common.models.base_context import ContextBase
from src.common.spark.spark_service import SparkService
from src.silver.builders.model_director import ModelDirector
from src.silver.builders.silver_model_builder import SilverModelBuilder
from src.silver.context.silver_context import SilverContext

class SilverModule(DIModule):
    def __init__(self, context: SilverContext, spark_session: SparkService, config: ConfigManager):
        super().__init__(context, config)
        self._spark_session = spark_session


    def configure(self, binder: Binder):
        binder.install(BuildersModule(self._context, self._config))
        binder.install(ReadersModule(self._context, self._config))
        
        
        binder.bind(SilverContext, to=self.provide_context, scope=singleton)
        binder.bind(ContextBase, to=self.provide_context, scope=singleton)
        binder.bind(ModelDirector, to=ModelDirector, scope=singleton)
        binder.bind(SilverModelBuilder, to=SilverModelBuilder, scope=singleton)
        
        



    @singleton
    @provider
    def provide_spark_session(self) -> SparkService:
        return self._spark_session
    
    @singleton
    @provider
    def provide_context(self) -> SilverContext:
        return self._context