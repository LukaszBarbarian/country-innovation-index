# src/silver/data_readers/nobel_prize_data_reader.py
from ctypes import Structure
from pyspark.sql import DataFrame
from injector import inject
from src.common.models.ingestion_result import IngestionResult
from src.common.readers.base_data_reader import BaseDataReader
from src.common.enums.domain_source import DomainSource
from src.common.registers.data_reader_registry import DataReaderRegistry
from typing import Dict, Optional, cast, List
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, IntegerType, DoubleType, MapType
import pyspark.sql.functions as F

@DataReaderRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeDataReader(BaseDataReader):
    """
    Czytnik danych dla nagród Nobla, pobierający dane z warstwy Bronze.
    Wczytuje plik JSON z lokalizacji podanej w kontekście.
    """

    def _load_from_source(self, all_readers: Optional[List[IngestionResult]]) -> Dict[str, DataFrame]:
        if not all_readers:
            print("Brak wyników dla DomainSource.NOBELPRIZE w kontekście.")
            return {}
        
        dataframes_dict: Dict[str, DataFrame] = {}
        for result in all_readers:
            if result.is_valid:
                dataset_name = result.dataset_name
                
                for path in result.output_paths:
                    try:
                        if dataset_name == "laureates":
                            # Używamy zaktualizowanego schematu
                            schema = self._get_schema_laureates()
                            
                            df = self._spark.read_json(path, schema=schema)
                            
                            dataframes_dict[dataset_name] = df
                            print(f"Załadowano dane dla datasetu '{dataset_name}' z pliku: {path}")
                    
                    except Exception as e:
                        print(f"Błąd podczas ładowania pliku {path}: {e}")
        
        return dataframes_dict

    def _get_schema_laureates(self) -> StructType:
        multi_lang_string_schema = StructType([
            StructField("en", StringType(), True),
            StructField("se", StringType(), True),
            StructField("no", StringType(), True)
        ])

        place_schema = StructType([
            StructField("city", multi_lang_string_schema, True),
            StructField("country", multi_lang_string_schema, True),
            StructField("cityNow", multi_lang_string_schema, True),
            StructField("countryNow", multi_lang_string_schema, True),
            StructField("continent", multi_lang_string_schema, True),
            StructField("locationString", multi_lang_string_schema, True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("sameAs", ArrayType(StringType()), True)
        ])

        birth_death_schema = StructType([
            StructField("date", StringType(), True),
            StructField("place", place_schema, True)
        ])

        affiliation_schema = StructType([
            StructField("name", multi_lang_string_schema, True),
            StructField("nameNow", multi_lang_string_schema, True),
            StructField("city", multi_lang_string_schema, True),
            StructField("country", multi_lang_string_schema, True),
            StructField("locationString", multi_lang_string_schema, True),
            StructField("cityNow", place_schema, True),
            StructField("countryNow", place_schema, True),
            StructField("continent", multi_lang_string_schema, True)
        ])

        nobel_prize_schema = StructType([
            StructField("awardYear", StringType(), True),
            StructField("category", multi_lang_string_schema, True),
            StructField("categoryFullName", multi_lang_string_schema, True),
            StructField("sortOrder", StringType(), True),
            StructField("portion", StringType(), True),
            StructField("dateAwarded", StringType(), True),
            StructField("prizeStatus", StringType(), True),
            StructField("motivation", multi_lang_string_schema, True),
            StructField("prizeAmount", LongType(), True),
            StructField("prizeAmountAdjusted", LongType(), True),
            StructField("affiliations", ArrayType(affiliation_schema), True),
            StructField("links", ArrayType(MapType(StringType(), StringType())), True)
        ])

        # Schemat **pojedynczego laureata**, bez zewnętrznej tablicy
        laureate_schema = StructType([
            StructField("id", StringType(), True),
            StructField("knownName", multi_lang_string_schema, True),
            StructField("givenName", multi_lang_string_schema, True),
            StructField("familyName", multi_lang_string_schema, True),
            StructField("fullName", multi_lang_string_schema, True),
            StructField("fileName", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("birth", birth_death_schema, True),
            StructField("death", birth_death_schema, True),
            StructField("wikipedia", MapType(StringType(), StringType()), True),
            StructField("wikidata", MapType(StringType(), StringType()), True),
            StructField("sameAs", ArrayType(StringType()), True),
            StructField("links", ArrayType(MapType(StringType(), StringType())), True),
            StructField("nobelPrizes", ArrayType(nobel_prize_schema), True)
        ])

        return laureate_schema
