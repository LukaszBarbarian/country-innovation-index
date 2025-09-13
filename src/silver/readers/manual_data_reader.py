# src/silver/loaders/manual_data_loader.py
from typing import Optional, Dict
from injector import inject
from pyspark.sql import DataFrame
import os

from src.common.enums.domain_source import DomainSource
from src.common.spark.spark_service import SparkService
from src.silver.context.silver_context import SilverContext


class ManualDataReader:
    """
    A data reader responsible for loading manual datasets for the Silver layer.

    This class reads data from files specified in the manifest, handling
    different file formats (JSON, CSV) based on their extensions. It
    uses the `SilverContext` to locate the correct file paths.
    """
    @inject
    def __init__(self, spark: SparkService, context: SilverContext):
        """
        Initializes the reader with a Spark session and the Silver context.
        """
        self._context = context
        self._spark = spark

    def load_for_dataset(self, domain_source: DomainSource, dataset_name: str) -> Optional[DataFrame]:
        """
        Loads a specific manual dataset for a given domain and returns a DataFrame.
        It supports JSON and CSV files based on the file extension.

        Args:
            domain_source (DomainSource): The source domain of the manual data.
            dataset_name (str): The name of the specific dataset.

        Returns:
            Optional[DataFrame]: The loaded Spark DataFrame, or None if the file
                                 is not found or an error occurs.
        """
        file_path = None
        # Look for the file path based on the domain and dataset name
        for path in self._context.manifest.manual_data_paths:
            if path.domain_source == domain_source and path.dataset_name == dataset_name:
                file_path = path.file_path
                break

        if not file_path:
            print(f"Error: Path not found for manual dataset: '{domain_source.name}:{dataset_name}'.")
            return None
        
        try:
            # Determine the file extension to choose the correct read method
            _, file_extension = os.path.splitext(file_path)
            
            if file_extension.lower() == '.json':
                df = self._spark.read_json_https(file_path)
            elif file_extension.lower() == '.csv':
                # Add inferSchema and header options to automatically detect data types and headers
                df = self._spark.read_csv_https(file_path)
            else:
                print(f"Error: Unsupported file extension '{file_extension}' for file '{file_path}'.")
                return None
            
            print(f"Loaded manual dataset '{dataset_name}' from file: {file_path}")
            return df

        except Exception as e:
            print(f"Error loading manual file '{file_path}': {e}")
            return None