# src/silver/di/silver_module.py

from injector import Module, Binder, singleton, provider
from pyspark.sql import SparkSession
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import BaseContext

from src.common.di.di_module import DIModule
from src.common.di.builders_module import BuildersModule
from src.common.di.readers_module import ReadersModule
from src.common.spark.spark_service import SparkService
from src.silver.builders.model_director import ModelDirector
from src.silver.context.silver_context import SilverLayerContext

class SilverModule(DIModule):
    def __init__(self, context: SilverLayerContext, spark_session: SparkService, config: ConfigManager):
        super().__init__(context, config)
        self._spark_session = spark_session


    def configure(self, binder: Binder):
        binder.install(BuildersModule(self._context, self._config))
        binder.install(ReadersModule(self._context, self._config))
        
        
        binder.bind(SilverLayerContext, to=self.provide_context, scope=singleton)
        binder.bind(BaseContext, to=self.provide_context, scope=singleton)
        binder.bind(ModelDirector, to=ModelDirector, scope=singleton)
        



    @singleton
    @provider
    def provide_spark_session(self) -> SparkService:
        return self._spark_session
    
    @singleton
    @provider
    def provide_context(self) -> SilverLayerContext:
        return self._context