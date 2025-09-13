# src/silver/loaders/reference_loader.py
from typing import Optional
from injector import inject
from pyspark.sql import DataFrame
import os

from src.common.enums.reference_source import ReferenceSource
from src.common.spark.spark_service import SparkService
from src.silver.context.silver_context import SilverContext


class ReferenceDataReader:
    """
    A data reader responsible for loading reference datasets for the Silver layer.
    
    This class reads data from reference tables specified in the manifest,
    which are typically used for lookups and data enrichment. It leverages the
    `SilverContext` to find the correct file paths.
    """
    @inject
    def __init__(self, spark: SparkService, context: SilverContext):
        """
        Initializes the reader with a Spark session and the Silver context.
        """
        self._context = context
        self._spark = spark

    def load_for_dataset(self, dataset: ReferenceSource) -> Optional[DataFrame]:
        """
        Loads a specific reference dataset from a file and returns a DataFrame.
        This method is synchronous and does not use `asyncio`.

        Args:
            dataset (ReferenceSource): The enum representing the reference dataset to load.

        Returns:
            Optional[DataFrame]: The loaded Spark DataFrame, or None if the path
                                 is not found or an error occurs.
        """
        file_path = self._context.manifest.references_tables.get(dataset)
        
        if not file_path:
            print(f"Error: Path not found for reference dataset: '{dataset}'.")
            return None
        
        try:
            # 2. Use Spark to read the file
            df = self._spark.read_csv_https(file_path)
            return df

        except Exception as e:
            print(f"Error loading reference file '{file_path}': {e}")
            return None