# src/silver/di/silver_module.py

from injector import Module, Binder, singleton, provider
from pyspark.sql import SparkSession
from src.common.utils.cache import Cache
from src.common.contexts.layer_context import LayerContext

from src.common.di.di_module import DIModule
from src.common.di.builders_module import BuildersModule
from src.common.di.readers_module import ReadersModule

class SilverModule(DIModule):
    def __init__(self, context: LayerContext, spark_session: SparkSession):
        super().__init__(context)
        self._spark_session = spark_session


    def configure(self, binder: Binder):
        binder.install(BuildersModule(self._context))
        binder.install(ReadersModule(self._context))
        
        binder.bind(LayerContext, to=self.provide_context, scope=singleton)


    @singleton
    @provider
    def provide_spark_session(self) -> SparkSession:
        return self._spark_session
    
    @singleton
    @provider
    def provide_context(self) -> LayerContext:
        return self._context