# src/silver/builders/graduate_unemployment_model_builder.py
from injector import inject
import pyspark.sql.functions as F
from typing import Dict, Tuple
from pyspark.sql import DataFrame

from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.GRADUATE_UNEMPLOYMENT)
class GraduateUnemploymentModelBuilder(SilverModelBuilder):
    """
    A model builder for the 'graduate_unemployment' model in the Silver layer.
    
    This builder combines unemployment data from the World Bank with a master
    list of countries to create a structured and enriched dataset. It ensures
    that only countries present in the country dimension are included.
    """
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        """
        Builds the 'graduate_unemployment' model by joining unemployment data
        with the country dimension table.

        Args:
            datasets (Dict[Tuple, DataFrame]): A dictionary of loaded DataFrames from various sources.
            dependencies (Dict[ModelType, DataFrame]): A dictionary of DataFrames from other Silver layer models.

        Returns:
            DataFrame: An enriched Spark DataFrame for graduate unemployment.

        Raises:
            ValueError: If the 'COUNTRY' model or 'unemployment' dataset is missing.
        """
        # 1. Get the country model from dependencies
        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None or country_df.count() == 0:
            raise ValueError("The country model (COUNTRY) is missing from dependencies.")

        # 2. Get the unemployment data frame from datasets
        unemployment_df = datasets.get((DomainSource.WORLDBANK, "unemployment"))
        if not unemployment_df or unemployment_df.count() == 0:
            raise ValueError("No data found in the 'unemployment' data frame.")

        # 3. Normalize the unemployment data columns
        # As per the WorldBankTransformer, the input data should contain the 'ISO3166-1-Alpha-3'
        # and 'unemployment_rate' columns.
        unemployment_df = unemployment_df.select(
            F.col("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("unemployment_rate")
        )

        # 4. Join with the country model on ISO
        final_df = unemployment_df.join(
            country_df,
            unemployment_df["ISO3166-1-Alpha-3"] == country_df["ISO3166-1-Alpha-3"],
            "inner"  # Inner join returns only countries from the reference list.
        ).select(
            unemployment_df["ISO3166-1-Alpha-3"].alias("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("unemployment_rate"),
            F.col("country_name"),
            F.col("ref_worldbank")  # Flag indicating that the country exists in the World Bank data
        )

        return final_df