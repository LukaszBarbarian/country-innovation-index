# azure_databricks/common/data_reader.py

from pyspark.sql import SparkSession, DataFrame
from typing import Any, Dict, Optional

from src.common.enums.file_format import FileFormat
from src.azure_databricks.common.configuration.config import ProjectConfig # Jeśli potrzebujesz dostępu do configa

class DataReader:
    def __init__(self, spark: SparkSession, config: ProjectConfig):
        self.spark = spark
        self.config = config # Możesz przekazać config, jeśli np. ścieżki do plików surowych są tam zdefiniowane
        print("DataReader zainicjowany.")

    def read_data(self, path: str, file_format: FileFormat, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        """
        Odczytuje dane z podanej ścieżki i formatu.
        
        Args:
            path (str): Pełna ścieżka do danych (np. abfss://raw@...).
            file_format (FileFormat): Format pliku (np. FileFormat.CSV, FileFormat.JSON).
            options (Optional[Dict[str, Any]]): Dodatkowe opcje odczytu dla danego formatu.
        
        Returns:
            DataFrame: Odczytany DataFrame Sparka.
        """
        if options is None:
            options = {}
        
        reader = self.spark.read.format(file_format.value)
        
        for key, value in options.items():
            reader = reader.option(key, value)
            
        print(f"Odczytuję dane z '{path}' w formacie '{file_format.value}' z opcjami: {options}")
        return reader.load(path)


    def read_streaming_data(self, path: str, file_format: FileFormat, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        pass