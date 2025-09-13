# src/common/builders/base_model_builder.py
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from injector import inject, Injector

from src.common.config.config_manager import ConfigManager
from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.models.base_context import ContextBase
from src.common.models.base_process import BaseProcessModel
from src.common.spark.spark_service import SparkService


class BaseModelBuilder(ABC):
    """
    An abstract base class for building data models.
    It provides a standardized structure for data loading, processing,
    and creating a final model object. Subclasses must implement the
    abstract methods to define their specific logic.
    """
    @inject
    def __init__(
        self,
        spark_service: SparkService,
        injector: Injector,
        context: ContextBase,
        config: ConfigManager
    ):
        """
        Initializes the builder with injected dependencies.

        Args:
            spark_service (SparkService): The Spark service for managing Spark sessions.
            injector (Injector): The dependency injector instance.
            context (ContextBase): The context object for the current process.
            config (ConfigManager): The configuration manager for application settings.
        """
        self._spark_service = spark_service
        self._injector = injector
        self._context = context
        self._config = config
        self._model_type: Optional[ModelType] = None
        self.synthetic = False

    def set_identity(self, model_type: ModelType):
        """
        Sets the identity of the model being built.
        
        Args:
            model_type (ModelType): An enumeration value representing the type of the model.
        """
        self._model_type = model_type

    @abstractmethod
    def load_data(self) -> Dict[DomainSource, Dict[str, DataFrame]]:
        """
        Abstract method to load source data for the model.

        Subclasses must implement this to define how raw data is loaded from
        various sources.

        Returns:
            Dict[DomainSource, Dict[str, DataFrame]]: A dictionary of Spark DataFrames,
                                                      organized by their domain source.
        """
        raise NotImplementedError

    @abstractmethod
    async def build(
        self,
        datasets: Dict[DomainSource, Dict[str, DataFrame]],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        """
        Abstract method to build the model's DataFrame.

        Subclasses must implement this to perform data transformations, joins,
        and other logic to create the final DataFrame.

        Args:
            datasets (Dict): The raw datasets loaded by `load_data`.
            dependencies (Dict): Any dependent models (pre-built DataFrames) required for this model.
        
        Returns:
            DataFrame: The final, processed Spark DataFrame for the model.
        """
        raise NotImplementedError

    def get_partition_cols(self) -> Optional[List[str]]:
        """
        Returns a list of column names to be used for partitioning the output DataFrame.

        This is an optional method that can be overridden by subclasses.

        Returns:
            Optional[List[str]]: A list of partition column names, or None if no partitioning is needed.
        """
        return None


    @abstractmethod
    def create_model(self, df: DataFrame) -> BaseProcessModel:
        """
        Abstract method to create the final model object from the processed DataFrame.

        Subclasses must implement this to instantiate a specific `BaseProcessModel`
        with the built DataFrame and other relevant metadata.

        Args:
            df (DataFrame): The final DataFrame produced by the `build` method.
        
        Returns:
            BaseProcessModel: An instance of the model object.
        """
        raise NotImplementedError