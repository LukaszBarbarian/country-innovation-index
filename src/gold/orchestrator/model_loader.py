# src/common/services/model_loader.py
import logging
from injector import inject
from pyspark.sql import DataFrame
from src.common.spark.spark_service import SparkService

logger = logging.getLogger(__name__)

class ModelLoader:
    """
    A service for loading analytical models (Delta tables) from a given URL
    (e.g., abfss://...). It uses dependency injection to get the SparkService.
    """

    @inject
    def __init__(self, spark: SparkService):
        """
        Initializes the ModelLoader with a SparkService instance.
        """
        self._spark = spark

    def load(self, url: str) -> DataFrame:
        """
        Loads a Spark DataFrame from the given URL (abfss, dbfs, file, etc.).

        Args:
            url (str): The path to the data source.

        Returns:
            DataFrame: The loaded Spark DataFrame.

        Raises:
            Exception: If there is an error loading the model.
        """
        logger.info(f"Loading model from {url}")
        try:
            df = self._spark.read_delta_abfss(url)
            return df
        except Exception as e:
            logger.error(f"Error loading model from {url}: {e}")
            raise