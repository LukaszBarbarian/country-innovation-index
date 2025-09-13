from typing import Dict, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.POPULATION)
class PopulationModelBuilder(SilverModelBuilder):
    """
    A model builder for the 'population' model in the Silver layer.
    
    This builder takes raw population data from the World Bank and enriches it by joining
    with the master country dimension. This ensures the final dataset is
    clean, consistent, and contains data only for countries listed in the master
    country dimension.
    """
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        """
        Builds the 'population' model by joining population data with the country dimension table.

        Args:
            datasets (Dict[Tuple, DataFrame]): A dictionary of loaded DataFrames from various sources.
            dependencies (Dict[ModelType, DataFrame]): A dictionary of DataFrames from other Silver layer models.

        Returns:
            DataFrame: An enriched Spark DataFrame for population by country and year.

        Raises:
            ValueError: If the 'COUNTRY' model or 'population' dataset is missing.
        """
        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None or country_df.count() == 0:
            raise ValueError("The country model (COUNTRY) is missing from dependencies.")

        # 2. Get the population data frame from datasets
        population_df = datasets.get((DomainSource.WORLDBANK, "population"))
        if not population_df or population_df.count() == 0:
            raise ValueError("No data found in the 'population' data frame.")

        # 3. Normalize the population data columns
        # As per the WorldBankTransformer, the input data should contain 'ISO3166-1-Alpha-3', 'year', and 'value'.
        population_df = population_df.select(
            F.col("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value")
        )

        # 4. Join with the country model on ISO
        final_df = population_df.join(
            country_df,
            population_df["ISO3166-1-Alpha-3"] == country_df["ISO3166-1-Alpha-3"],
            "inner"  # Inner join returns only countries from the reference list.
        ).select(
            population_df["ISO3166-1-Alpha-3"].alias("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value"),
            F.col("country_name"),
            F.col("ref_worldbank")  # Flag indicating that the country exists in the World Bank data
        )

        return final_df