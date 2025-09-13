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
    """
    A dependency injection module for the Silver ETL layer.
    
    This module configures the bindings for various services and builders required
    to run the Silver layer ETL process. It ensures that components like the Spark
    session, context, and the ModelDirector are available as singletons.
    """
    def __init__(self, context: SilverContext, spark_session: SparkService, config: ConfigManager):
        """
        Initializes the module with the necessary context, Spark session, and configuration.
        """
        super().__init__(context, config)
        self._spark_session = spark_session

    def configure(self, binder: Binder):
        """
        Configures the dependency bindings for the Silver layer.
        
        Args:
            binder (Binder): The Injector binder to configure.
        """
        # Install sub-modules for builders and readers
        binder.install(BuildersModule(self._context, self._config))
        binder.install(ReadersModule(self._context, self._config))
        
        # Bind core Silver layer components as singletons
        binder.bind(SilverContext, to=self.provide_context, scope=singleton)
        binder.bind(ContextBase, to=self.provide_context, scope=singleton)
        binder.bind(ModelDirector, to=ModelDirector, scope=singleton)
        binder.bind(SilverModelBuilder, to=SilverModelBuilder, scope=singleton)
        
    @singleton
    @provider
    def provide_spark_session(self) -> SparkService:
        """
        Provides the SparkService instance as a singleton.
        """
        return self._spark_session
    
    @singleton
    @provider
    def provide_context(self) -> SilverContext:
        """
        Provides the SilverContext instance as a singleton.
        """
        return self._context