# src/silver/builders/nobel_laureates_model_builder.py
from injector import inject
import pyspark.sql.functions as F
from typing import Dict, Tuple
from pyspark.sql import DataFrame

from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.NOBEL_LAUREATES)
class NobelLaureatesModelBuilder(SilverModelBuilder):
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        # 1. Pobierz model krajów
        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None or country_df.count() == 0:
            raise ValueError("Brak modelu krajów (COUNTRY) w zależnościach.")

        # 2. Pobierz dane o laureatach Nobla
        laureates_df = datasets.get((DomainSource.NOBELPRIZE, "laureates"))
        if not laureates_df or laureates_df.count() == 0:
            raise ValueError("Brak danych dla 'laureates'.")

        # 3. Join po znormalizowanej nazwie kraju
        final_df = laureates_df.join(
            country_df,
            laureates_df["country_normalized"] == country_df["country_code"],
            "inner"
        ).select(
            laureates_df["laureate_id"],
            laureates_df["year"],
            F.col("country").alias("laureate_country_name"),
            F.col("ISO3166-1-Alpha-3"),
            F.col("country_name").alias("country_name_ref")
        )

        return final_df
