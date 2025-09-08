# src/silver/loaders/reference_loader.py
from typing import Optional
from injector import inject
from pyspark.sql import DataFrame
import os

from src.common.enums.reference_source import ReferenceSource
from src.common.spark.spark_service import SparkService
from src.silver.context.silver_context import SilverContext

class ReferenceDataReader:
    @inject
    def __init__(self, spark: SparkService, context: SilverContext):
        self._context = context
        self._spark = spark

    def load_for_dataset(self, dataset: ReferenceSource) -> Optional[DataFrame]:
        """
        Ładuje konkretny dataset referencyjny z plików i zwraca DataFrame.
        Metoda jest synchroniczna i nie używa asyncio.
        """

        file_path = self._context.manifest.references_tables.get(dataset)
        
        if not file_path:
            print(f"Błąd: Nie znaleziono ścieżki dla datasetu referencyjnego: '{dataset}'.")
            return None
        
        try:
            # 2. Używamy Sparka do odczytu pliku
            df = self._spark.read_csv_https(file_path)
            return df

        except Exception as e:
            print(f"Błąd podczas ładowania pliku referencyjnego '{file_path}': {e}")
            return None