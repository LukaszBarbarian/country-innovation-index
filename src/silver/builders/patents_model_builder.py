# src/silver/model_builders/patents_model_builder.py
import pyspark.sql.functions as F
from typing import Dict, Tuple
from pyspark.sql import DataFrame
from injector import inject

from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.PATENTS)
class PatentsModelBuilder(SilverModelBuilder):
    """
    A model builder for the 'patents' model in the Silver layer.
    
    This builder aggregates raw patent data by country and year, then joins it with the
    master country list to enrich the data with ISO codes and official country names.
    """
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        """
        Builds the 'patents' model by aggregating raw data and joining it with
        the country dimension.

        Args:
            datasets (Dict[Tuple, DataFrame]): A dictionary of loaded DataFrames from various sources.
            dependencies (Dict[ModelType, DataFrame]): A dictionary of DataFrames from other Silver layer models.

        Returns:
            DataFrame: An enriched and aggregated Spark DataFrame for patents.

        Raises:
            ValueError: If the source 'patents' data or the 'country' dependency is missing.
        """
        patents_df = datasets.get((DomainSource.PATENTS, "patents"))
        if not patents_df:
            raise ValueError("Missing source data for 'patents'.")

        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None:
            raise ValueError("Missing dependency: 'country' model.")

        # 🔹 Aggregate by Origin (=country_code) and Year
        aggregated_df = (
            patents_df.groupBy("country_code", "year")
            .agg(
                F.sum("patents_count").alias("patents_total"),
                F.sum("resident_patents").alias("resident_patents"),
                F.sum("abroad_patents").alias("abroad_patents")
            )
        )

        # 🔹 Join with the country model to get ISO3166-1-Alpha-3
        result_df = aggregated_df.join(
            country_df,
            aggregated_df["country_code"] == country_df["country_code"],
            "inner"
        )

        # 🔹 Select final columns
        final_df = result_df.select(
            country_df["country_name"],
            country_df["ISO3166-1-Alpha-3"],
            aggregated_df["year"],
            aggregated_df["patents_total"],
            aggregated_df["resident_patents"],
            aggregated_df["abroad_patents"]
        )

        return final_df