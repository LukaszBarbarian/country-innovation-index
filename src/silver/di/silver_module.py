# src/silver/di/silver_module.py

from injector import Module, Binder, singleton, provider
from pyspark.sql import SparkSession
from src.common.config.config_manager import ConfigManager
from src.common.contexts.base_layer_context import BaseLayerContext

from src.common.di.di_module import DIModule
from src.common.di.builders_module import BuildersModule
from src.common.di.readers_module import ReadersModule

class SilverModule(DIModule):
    def __init__(self, context: BaseLayerContext, spark_session: SparkSession, config: ConfigManager):
        super().__init__(context, config)
        self._spark_session = spark_session


    def configure(self, binder: Binder):
        binder.install(BuildersModule(self._context, self._config))
        binder.install(ReadersModule(self._context, self._config))
        
        binder.bind(BaseLayerContext, to=self.provide_context, scope=singleton)


    @singleton
    @provider
    def provide_spark_session(self) -> SparkSession:
        return self._spark_session
    
    @singleton
    @provider
    def provide_context(self) -> BaseLayerContext:
        return self._context