from ctypes import Structure
from typing import Dict, List, Any, Optional
from pandas import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.models.ingestion_result import IngestionResult
from src.common.models.models import SummaryResultBase
from src.common.readers.base_data_reader import BaseDataReader
from src.common.registers.data_reader_registry import DataReaderRegistry
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType


@DataReaderRegistry.register(DomainSource.WORLDBANK)
class WorldbankDataReader(BaseDataReader):
    """
    A data reader for World Bank data, loading JSON files from the Bronze layer.
    
    This reader retrieves data for various indicators like population, R&D expenditure,
    and GDP, applying a specific schema for each dataset to ensure correct data
    types and structure.
    """
    def _load_from_source(self, all_readers: Optional[List[SummaryResultBase]]) -> Dict[str, DataFrame]:
        """
        Loads data from JSON files for a given DomainSource,
        using the appropriate schema for each dataset.
        
        Args:
            all_readers (Optional[List[SummaryResultBase]]): A list of summary results
                                                            from the Bronze layer run.
        
        Returns:
            Dict[str, DataFrame]: A dictionary of loaded DataFrames, keyed by dataset name.
        """
        if not all_readers:
            print(f"No results for DomainSource.{DomainSource.WORLDBANK.value} in the context.")
            return {}

        dataframes_dict: Dict[str, Any] = {}
        
        for result in all_readers:
            dataset_name = result.dataset_name
            
            schema = self._get_schema(dataset_name)
            
            if schema is None:
                print(f"No schema defined for dataset: '{dataset_name}'. Skipping.")
                continue

            for path in result.output_paths:
                try:
                    df = self._spark.read_json_https(path, schema=schema)
                    
                    dataframes_dict[dataset_name] = df
                    print(f"Loaded data for dataset '{dataset_name}' from file: {path}")

                except Exception as e:
                    print(f"Error loading file {path}: {e}")
        
        return dataframes_dict

    def _get_schema(self, dataset_name: str) -> StructType:
        """
        Returns the appropriate Spark schema for a given dataset.
        """
        if dataset_name == "population":
            return self._get_population_schema()
        elif dataset_name == "researchers":
            return self._get_researches_schema()
        elif dataset_name == "unemployment":
            return self._get_unemployment_schema()
        elif dataset_name == "rd":
            return self._get_rd_schema()
        elif dataset_name == "pkb":
            return self._get_pkb_schema()
        else:
            return None

    def _get_population_schema(self) -> StructType:
        """
        Schema for population data.
        """
        indicator_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        country_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])

        return StructType([
            StructField("indicator", indicator_schema, True),
            StructField("country", country_schema, True),
            StructField("countryiso3code", StringType(), True),
            StructField("date", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("obs_status", StringType(), True),
            StructField("decimal", IntegerType(), True)
        ])
    
    def _get_researches_schema(self) -> StructType:
        """
        Schema for researchers data.
        """
        indicator_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])

        country_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        return StructType([
            StructField("indicator", indicator_schema, True),
            StructField("country", country_schema, True),
            StructField("countryiso3code", StringType(), True),
            StructField("date", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("obs_status", StringType(), True),
            StructField("decimal", IntegerType(), True)
        ])
    
    def _get_unemployment_schema(self) -> StructType:
        """
        Schema for unemployment data.
        """
        indicator_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])

        country_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        return StructType([
            StructField("indicator", indicator_schema, True),
            StructField("country", country_schema, True),
            StructField("countryiso3code", StringType(), True),
            StructField("date", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("obs_status", StringType(), True),
            StructField("decimal", IntegerType(), True)
        ])
    
    def _get_rd_schema(self) -> StructType:
        """
        Schema for R&D expenditure data (% of GDP).
        """
        indicator_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])

        country_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        return StructType([
            StructField("indicator", indicator_schema, True),
            StructField("country", country_schema, True),
            StructField("countryiso3code", StringType(), True),
            StructField("date", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("obs_status", StringType(), True),
            StructField("decimal", IntegerType(), True)
        ])

    def _get_pkb_schema(self) -> StructType:
        """
        Schema for GDP data (current USD).
        """
        indicator_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])

        country_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        return StructType([
            StructField("indicator", indicator_schema, True),
            StructField("country", country_schema, True),
            StructField("countryiso3code", StringType(), True),
            StructField("date", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("obs_status", StringType(), True),
            StructField("decimal", IntegerType(), True)
        ])