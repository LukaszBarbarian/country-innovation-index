import asyncio
from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import SparkSession

class BaseETLOrchestrator(ABC):
    def __init__(self, spark: SparkSession, dbutils_obj: Any, env: str = "dev"):
        self.spark = spark
        self.dbutils_obj = dbutils_obj
        self.env = env
    
    @abstractmethod
    async def run(self):
        """
        Metoda abstrakcyjna do uruchamiania procesów ETL.
        Każdy konkretny orkiestrator musi zaimplementować tę metodę.
        """
        pass

    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return self.__str__()