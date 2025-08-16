# src/silver/model_builders/country_model_builder.py
from typing import Dict, cast
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from injector import inject
from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.registers.model_builder_registry import ModelBuilderRegistry


@ModelBuilderRegistry.register(ModelType.COUNTRY)
class CountryModelBuilder(BaseModelBuilder):
    
    async def _load_data(self) -> Dict[str, DataFrame]:
        nobelprize_reader = self.get_reader(DomainSource.NOBELPRIZE)
        return nobelprize_reader.load_data()

    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        if dataset_name == "laureates":
            # 1. Rozbijamy (eksplodujemy) tablicę laureatów na pojedyncze wiersze
            exploded_df = df.select(F.explode("laureates").alias("laureate"))
            
            # 2. Wybieramy tylko kolumnę z krajem urodzenia
            country_df = exploded_df.select(F.col("laureate.birth.place.countryNow.en").alias("birth_country"))
            
            # 3. Wybieramy tylko unikalne wartości z tej kolumny
            # Ta operacja tworzy DataFrame z jedną kolumną 'birth_country' i tylko unikalnymi wartościami
            distinct_countries_df = country_df.distinct()
            
            return distinct_countries_df
        else:
            return df

    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        # Tutaj możesz dodać logikę normalizacji dla różnych datasetów
        return df

    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        # Tutaj możesz dodać logikę wzbogacania dla różnych datasetów
        return df

    async def combine(self, dataframes: Dict[str, DataFrame]) -> DataFrame:
        """
        Łączy przetworzone DataFrames w jeden finalny.
        W tym przypadku, jeśli mamy nagrody i laureatów, połącz je.
        """
        laureates_df = dataframes.get("laureates")
        
        if laureates_df:
            return laureates_df
        else:
            raise ValueError("Brak danych do połączenia.")