from ctypes import Structure
from typing import Dict, List, Any, Optional
from pandas import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.models.ingestion_result import IngestionResult
from src.common.readers.base_data_reader import BaseDataReader
from src.common.registers.data_reader_registry import DataReaderRegistry
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType


@DataReaderRegistry.register(DomainSource.WORLDBANK)
class WorldbankDataReader(BaseDataReader):
    def _load_from_source(self, all_readers: Optional[List[IngestionResult]]) -> Dict[str, DataFrame]:
        """
        Ładuje dane z plików JSON dla danego DomainSource,
        używając odpowiedniego schematu dla każdego datasetu.
        """
        if not all_readers:
            print(f"Brak wyników dla DomainSource.{DomainSource.WORLDBANK.value} w kontekście.")
            return {}

        dataframes_dict: Dict[str, Any] = {}
        
        for result in all_readers:
            if result.is_valid:
                dataset_name = result.dataset_name
                
                schema = self._get_schema(dataset_name)
                
                if schema is None:
                    print(f"Brak zdefiniowanego schematu dla datasetu: '{dataset_name}'. Pomijam.")
                    continue

                for path in result.output_paths:
                    try:
                        df = self._spark.read_json(path, schema=schema)
                        
                        dataframes_dict[dataset_name] = df
                        print(f"Załadowano dane dla datasetu '{dataset_name}' z pliku: {path}")

                    except Exception as e:
                        print(f"Błąd podczas ładowania pliku {path}: {e}")
        
        return dataframes_dict

    def _get_schema(self, dataset_name: str) -> StructType:
        """Zwraca odpowiedni schemat Spark dla danego datasetu."""
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
        """Schemat dla danych o populacji."""
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
        """Schemat dla danych o badaczach."""
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
        """Schemat dla danych o bezrobociu."""
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
        """Schemat dla danych o wydatkach na badania i rozwój (% PKB)."""
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
        """Schemat dla danych o PKB (w bieżących USD)."""
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