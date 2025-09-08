from abc import ABC, abstractmethod

from injector import inject
from src.common.config.config_manager import ConfigManager
from src.common.models.build_request import BuildRequest
from pyspark.sql import DataFrame
from src.common.spark.spark_service import SparkService

class AnalyticalBaseBuilder(ABC):
    @inject
    def __init__(self, spark_service: SparkService, config=ConfigManager):
        self.spark_service = spark_service
        self.config = config

    @abstractmethod
    async def run(self, request: BuildRequest) -> DataFrame:
        """
        Powinien zwrócić ProcessModel z built DataFrame.
        Implementacje mogą być asynchroniczne (np. używają IO).
        """
        raise NotImplementedError
