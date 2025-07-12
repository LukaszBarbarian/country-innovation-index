import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Optional, Dict
from abc import ABC, abstractmethod


class DataLakeReader(ABC):
    def __init__(self, spark: SparkSession, target_container_path: str):
        if not isinstance(spark, SparkSession):
            raise TypeError("Parameter 'spark' needs to be SparkSession")
        
        self.spark = spark  
        self.target_container_path = target_container_path

    @abstractmethod
    def read(self) -> pd.DataFrame:
        pass


    def _read_with_format(self, path: str, file_format: str, options: Optional[Dict[str, str]]) -> DataFrame:
        try:
            reader = self.spark.read.format(file_format)

            if options:
                for key, value in options.items():
                    reader = reader.option(key, value)

            df = reader.load(path)
            df.show(5, truncate=False)
            df.printSchema()
            return df
        except Exception as e:        
            raise



    def json(self, path: str, options: Optional[Dict[str, str]] = None) -> DataFrame:
        default_options = {"multiline": "true", "inferSchema": "true"}
        final_options = {**default_options, **(options if options else {})}
        return self._read_with_format(path, "json", final_options)
    

    def csv(self, path: str, options: Optional[Dict[str, str]] = None) -> DataFrame:
        default_options = {"header": "true", "inferSchema": "true"}
        final_options = {**default_options, **(options if options else {})}
        return self._read_with_format(path, "csv", final_options)