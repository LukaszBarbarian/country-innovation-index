from abc import ABC, abstractmethod
from injector import inject
from src.common.config.config_manager import ConfigManager
from src.common.models.build_request import BuildRequest
from pyspark.sql import DataFrame
from src.common.spark.spark_service import SparkService

class AnalyticalBaseBuilder(ABC):
    """
    An abstract base class for all analytical data builders.
    It defines a standard interface and handles dependency injection for
    SparkService and ConfigManager.
    """
    @inject
    def __init__(self, spark_service: SparkService, config: ConfigManager):
        """
        Initializes the builder with injected dependencies.
        Args:
            spark_service (SparkService): An instance of the Spark service for
                                          managing Spark sessions and operations.
            config (ConfigManager): An instance of the configuration manager for
                                    accessing application settings.
        """
        self.spark_service = spark_service
        self.config = config

    @abstractmethod
    async def run(self, request: BuildRequest) -> DataFrame:
        """
        The abstract method that must be implemented by all subclasses.
        It should build a DataFrame based on the provided request and return it.
        This method is designed to be asynchronous to support non-blocking I/O operations.

        Args:
            request (BuildRequest): A data class containing parameters for the
                                    data building process.

        Returns:
            DataFrame: A PySpark DataFrame containing the processed and built data.
        
        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        raise NotImplementedError