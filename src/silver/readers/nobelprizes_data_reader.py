# src/silver/data_readers/nobel_prize_data_reader.py
from ctypes import Structure
from pyspark.sql import DataFrame
from injector import inject
from src.common.readers.base_data_reader import BaseDataReader
from src.common.enums.domain_source import DomainSource
from src.common.registers.data_reader_registry import DataReaderRegistry
from typing import Dict, cast, List
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType


# Importujemy konteksty, których używamy
from src.silver.context.silver_context import SilverLayerContext
from src.silver.context.silver_result_context import SilverResultContext

@DataReaderRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeDataReader(BaseDataReader):
    """
    Czytnik danych dla nagród Nobla, pobierający dane z warstwy Bronze.
    Wczytuje plik Parquet z lokalizacji podanej w kontekście.
    """

    def _load_from_source(self) -> Dict[str, DataFrame]:
        
        # 1. Sprawdzamy, czy kontekst jest prawidłowy i pobieramy wyniki dla naszego DomainSource
        if not isinstance(self._context, SilverLayerContext) or not self._context.is_valid:
            print("Kontekst Silver jest nieprawidłowy lub nie zawiera danych do załadowania.")
            return {}




        nobel_results = self._context.get_ingestion_results_for_domain_source(DomainSource.NOBELPRIZE)
        
        if not nobel_results:
            print("Brak wyników dla DomainSource.NOBELPRIZE w kontekście.")
            return {}
        

        dataframes_dict: Dict[str, DataFrame] = {}

        # 3. Iterujemy po wszystkich wynikach w kolekcji `results`
        for result in nobel_results:
            # Sprawdzamy, czy dany wynik jest dla naszego DomainSource
            if result.is_valid:
                dataset_name = result.dataset_name
                schema: Structure = None

                # 4. Iterujemy po ścieżkach z danego wyniku i ładujemy pliki
                for path in result.output_paths:
                    try:
                        if dataset_name == "laureates":
                            schema = self._get_schema_nobelprizes()

                        df = self._spark.read_json(path, schema)

                        
                        dataframes_dict[dataset_name] = df
                        print(f"Załadowano dane dla datasetu '{dataset_name}' z pliku: {path}")
                        
                    except Exception as e:
                        print(f"Błąd podczas ładowania pliku {path}: {e}")
                        
        return dataframes_dict
    

    def _get_schema_nobelprizes(self) -> Structure:
        countryNow_schema = StructType([
            StructField("en", StringType(), True),
            StructField("no", StringType(), True),
            StructField("se", StringType(), True),
            StructField("sameAs", ArrayType(StringType()), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)
        ])

        place_schema = StructType([
            StructField("countryNow", countryNow_schema, True),
            # Dodaj inne pola, jeśli potrzebujesz (np. city, cityNow)
        ])
        
        birth_schema = StructType([
            StructField("date", StringType(), True),
            StructField("place", place_schema, True)
        ])

        knownName_schema = StructType([
            StructField("en", StringType(), True),
            StructField("se", StringType(), True)
        ])

        laureate_schema = StructType([
            StructField("id", StringType(), True),
            StructField("knownName", knownName_schema, True),
            StructField("birth", birth_schema, True),
            StructField("gender", StringType(), True)
        ])

        return StructType([
            StructField("laureates", ArrayType(laureate_schema), True)
        ])
