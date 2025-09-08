# src/silver/model_builders/country_model_builder.py
import pyspark.sql.functions as F
from typing import Dict, Tuple
from pyspark.sql import DataFrame
from injector import inject

from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.enums.reference_source import ReferenceSource
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.COUNTRY)
class CountryModelBuilder(SilverModelBuilder):
    async def build(self, datasets: Dict[Tuple, DataFrame], dependencies: Dict[ModelType, DataFrame]) -> DataFrame:
        country_refs = self.get_references(ReferenceSource.COUNTRY_CODES)
        if not country_refs:
            raise ValueError("Brak danych referencyjnych 'country_codes'.")

        result_df = country_refs.select(
            F.col("ISO3166-1-Alpha-3"),
            F.col("official_name_en").alias("country_name")
        ).withColumn(
            "country_name_normalized",
            F.upper(F.trim(F.regexp_replace(F.col("country_name"), r"[^a-zA-Z\s]+", "")))
        ).withColumn(
            "ref_worldbank", F.lit(0)
        ).withColumn(
            "ref_nobelprize", F.lit(0)
        ).withColumn(
            "ref_patents", F.lit(0)
        )


        # Łączenie z World Bank
        worldbank_df = datasets.get((DomainSource.WORLDBANK, "population"))
        if worldbank_df and "ISO3166-1-Alpha-3" in worldbank_df.columns:
            worldbank_iso_df = worldbank_df.select(F.col("ISO3166-1-Alpha-3")).distinct()
            result_df = result_df.join(
                worldbank_iso_df.withColumn("ref_worldbank_flag", F.lit(1)),
                on="ISO3166-1-Alpha-3",
                how="left"
            ).withColumn(
                "ref_worldbank",
                F.when(F.col("ref_worldbank_flag").isNotNull(), 1).otherwise(F.col("ref_worldbank"))
            ).drop("ref_worldbank_flag")

        # Łączenie z Patents
        patents_df = datasets.get((DomainSource.PATENTS, "patents"))
        if patents_df and "country_code" in patents_df.columns:
            result_df = result_df.join(
                patents_df.select("country_code").distinct().withColumn("ref_patents_flag", F.lit(1)),
                result_df["country_name_normalized"] == patents_df["country_code"],
                how="left"
            ).withColumn(
                "ref_patents",
                F.when(F.col("ref_patents_flag").isNotNull(), 1).otherwise(0)
            ).drop("ref_patents_flag")

        # Łączenie z Nobel Prize
        nobel_df = datasets.get((DomainSource.NOBELPRIZE, "laureates"))
        if nobel_df and "country_normalized" in nobel_df.columns:
            nobel_countries_df = nobel_df.select("country_normalized").distinct()
            result_df = result_df.join(
                nobel_countries_df.withColumn("ref_nobelprize_flag", F.lit(1)),
                result_df.country_name_normalized == nobel_countries_df.country_normalized,
                how="left"
            ).withColumn(
                "ref_nobelprize",
                F.when(F.col("ref_nobelprize_flag").isNotNull(), 1).otherwise(F.col("ref_nobelprize"))
            ).drop("ref_nobelprize_flag", nobel_countries_df.country_normalized)

        return result_df.drop("country_name_normalized")