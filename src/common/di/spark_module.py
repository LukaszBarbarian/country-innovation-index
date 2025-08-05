# src/silver/di_modules/spark_module.py
from injector import Module, singleton, provider
from pyspark.sql import SparkSession
from src.common.di.di_module import DIModule


class SparkModule(DIModule):
    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session

    @singleton
    @provider
    def provide_spark_session(self) -> SparkSession:
        """
        Dostarcza instancję SparkSession jako singleton.
        """
        return self._spark_session