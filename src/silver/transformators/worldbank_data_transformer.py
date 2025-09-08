# src/common/transformers/world_bank_transformer.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.registers.domain_transformer_registry import DomainTransformerRegistry
from src.common.transformators.base_transformer import BaseTransformer


@DomainTransformerRegistry.register(DomainSource.WORLDBANK)
class WorldBankTransformer(BaseTransformer):
    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Transformuje dane WB, wybierając tylko kraj, kod ISO, rok i wartość wskaźnika.
        """
        # Wszystkie dataset-y WB mają podobną strukturę
        value_column_map = {
            "population": "value",
            "unemployment": "unemployment_rate",
            "researchers": "value",   # możesz później doprecyzować
            "rd": "value",
            "pkb": "value"
        }

        if dataset_name not in value_column_map:
            print(f"Ostrzeżenie: Brak transformacji dla zbioru danych '{dataset_name}'. Zwracam oryginalną ramkę danych.")
            return df

        value_col = value_column_map[dataset_name]

        # Transformacja: kraj, kod ISO, rok, wartość
        df = df.select(
            F.col("country.value").alias("country_name"),
            F.col("countryiso3code").alias("ISO3166-1-Alpha-3"),
            F.col("date").alias("year"),
            F.col("value").alias(value_col)
        )

        # Filtrowanie brakujących kodów ISO
        df = df.filter(F.col("ISO3166-1-Alpha-3").isNotNull())

        return df

    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        if "country_name" in df.columns:
            return df.withColumn(
                "country_name_normalized",
                F.upper(F.trim(F.regexp_replace(F.col("country_name"), r"[^a-zA-Z\s]+", "")))
            )
        return df

    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        return df
