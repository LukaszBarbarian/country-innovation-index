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
    """
    A model builder for the 'nobel_laureates' model in the Silver layer.

    This builder enriches raw Nobel laureates data by joining it with the
    master country dimension. It uses an inner join on a normalized country name
    to ensure that only valid countries are included in the final dataset.
    """
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        """
        Builds the 'nobel_laureates' model by combining raw Nobel Prize data
        with the country dimension table.

        Args:
            datasets (Dict[Tuple, DataFrame]): A dictionary of loaded DataFrames from various sources.
            dependencies (Dict[ModelType, DataFrame]): A dictionary of DataFrames from other Silver layer models.

        Returns:
            DataFrame: An enriched Spark DataFrame for Nobel laureates.

        Raises:
            ValueError: If the 'COUNTRY' model or 'laureates' dataset is missing.
        """
        # 1. Get the country model from dependencies
        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None or country_df.count() == 0:
            raise ValueError("The country model (COUNTRY) is missing from dependencies.")

        # 2. Get the Nobel laureates data
        laureates_df = datasets.get((DomainSource.NOBELPRIZE, "laureates"))
        if not laureates_df or laureates_df.count() == 0:
            raise ValueError("No data found for 'laureates'.")

        # 3. Join on normalized country name
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