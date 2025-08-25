# src/common/transformers/nobel_prize_transformer.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.registers.domain_transformer_registry import DomainTransformerRegistry
from src.common.transformators.base_transformer import BaseTransformer


@DomainTransformerRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeTransformer(BaseTransformer):
    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        if dataset_name == "laureates":
            # Rozwijamy tablicę nagród
            df = df.withColumn("prize", F.explode(F.col("nobelPrizes")))

            # Wybieramy tylko potrzebne kolumny
            df = df.select(
                F.col("id").alias("laureate_id"),
                F.coalesce(F.col("birth.place.country.en"), 
                           F.col("birth.place.countryNow.en")).alias("country"),
                F.col("prize.awardYear").alias("year")
            )

            # Filtrujemy puste kraje
            df = df.filter(F.col("country").isNotNull())

            return df

        return df

    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        if "country" in df.columns:
            return df.withColumn(
                "country_normalized",
                F.upper(F.trim(F.col("country")))
            )
        return df

    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        return df
