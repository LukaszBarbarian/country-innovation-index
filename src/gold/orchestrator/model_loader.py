# src/common/services/model_loader.py
import logging
from injector import inject
from pyspark.sql import DataFrame
from src.common.spark.spark_service import SparkService

logger = logging.getLogger(__name__)

class ModelLoader:
    """
    Serwis do ładowania modeli (Delta) z podanego URL (np. abfss://...).
    Wstrzykiwany SparkService przez DI.
    """

    @inject
    def __init__(self, spark: SparkService):
        self._spark = spark

    def load(self, url: str) -> DataFrame:
        """
        Ładuje DataFrame z podanego url (abfss, dbfs, file, itp.).
        """
        logger.info(f"Ładowanie modelu z {url}")
        try:
            df = self._spark.read_delta_abfss(url)
            return df
        except Exception as e:
            logger.error(f"Błąd przy ładowaniu modelu z {url}: {e}")
            raise
