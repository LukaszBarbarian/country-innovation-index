# src/silver/data_readers/nobel_prize_data_reader.py
from ctypes import Structure
from pyspark.sql import DataFrame
from injector import inject
from src.common.models.ingestion_result import IngestionResult
from src.common.models.models import SummaryResultBase
from src.common.readers.base_data_reader import BaseDataReader
from src.common.enums.domain_source import DomainSource
from src.common.registers.data_reader_registry import DataReaderRegistry
from typing import Dict, Optional, cast, List
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, IntegerType, DoubleType, MapType
import pyspark.sql.functions as F


@DataReaderRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeDataReader(BaseDataReader):
    """
    A data reader for Nobel Prize data, which retrieves data from the Bronze layer.
    It reads a JSON file from the location specified in the context.
    """

    def _load_from_source(self, all_readers: Optional[List[SummaryResultBase]]) -> Dict[str, DataFrame]:
        """
        Loads data from the source paths provided in the summary results.
        
        Args:
            all_readers (Optional[List[SummaryResultBase]]): A list of summary results
                                                            from the Bronze layer run.
        
        Returns:
            Dict[str, DataFrame]: A dictionary of loaded DataFrames, keyed by dataset name.
        """
        if not all_readers:
            print("No results for DomainSource.NOBELPRIZE in the context.")
            return {}
        
        dataframes_dict: Dict[str, DataFrame] = {}
        for result in all_readers:
            dataset_name = result.dataset_name
            
            for path in result.output_paths:
                try:
                    if dataset_name == "laureates":
                        # Use the updated schema
                        schema = self._get_schema_laureates()
                        
                        df = self._spark.read_json_https(path, schema=schema)
                        
                        dataframes_dict[dataset_name] = df
                        print(f"Loaded data for dataset '{dataset_name}' from file: {path}")
                
                except Exception as e:
                    print(f"Error loading file {path}: {e}")
    
        return dataframes_dict

    def _get_schema_laureates(self) -> StructType:
        """
        Defines the schema for the Nobel laureates JSON data.
        
        Returns:
            StructType: The Spark StructType schema for laureates.
        """
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

        # Schema for a single laureate, without the outer array
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