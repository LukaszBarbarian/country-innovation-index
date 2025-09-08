# src/silver/di/silver_module.py

from injector import Module, Binder, singleton, provider
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase
from src.common.di.di_module import DIModule
from src.common.spark.spark_service import SparkService
from src.gold.contexts.gold_layer_context import GoldContext
from src.gold.models.model_director import ModelDirector
from src.gold.orchestrator.model_loader import ModelLoader


class GoldModule(DIModule):
    def __init__(self, context: GoldContext, spark_session: SparkService, config: ConfigManager):
        super().__init__(context, config)
        self._spark_session = spark_session


    def configure(self, binder: Binder):             
        binder.bind(GoldContext, to=self.provide_context, scope=singleton)
        binder.bind(ContextBase, to=self.provide_context, scope=singleton)
        binder.bind(ModelDirector, to=ModelDirector, scope=singleton)
        binder.bind(ModelLoader, to=ModelLoader, scope=singleton)
        



    @singleton
    @provider
    def provide_spark_session(self) -> SparkService:
        return self._spark_session
    
    @singleton
    @provider
    def provide_context(self) -> GoldContext:
        return self._context