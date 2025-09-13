from typing import Dict, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.PKB)
class PKBModelBuilder(SilverModelBuilder):
    """
    A model builder for the 'pkb' (GDP) model in the Silver layer.
    
    This builder takes raw GDP data from the World Bank and enriches it by joining
    with the master country dimension. This ensures that the final dataset is
    clean, consistent, and only contains data for valid countries.
    """
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        """
        Builds the 'pkb' model by joining GDP data with the country dimension table.

        Args:
            datasets (Dict[Tuple, DataFrame]): A dictionary of loaded DataFrames from various sources.
            dependencies (Dict[ModelType, DataFrame]): A dictionary of DataFrames from other Silver layer models.

        Returns:
            DataFrame: An enriched Spark DataFrame for GDP by country and year.

        Raises:
            ValueError: If the 'COUNTRY' model or 'pkb' dataset is missing.
        """
        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None or country_df.count() == 0:
            raise ValueError("The country model (COUNTRY) is missing from dependencies.")

        # 2. Get the GDP data frame from datasets
        pkb_df = datasets.get((DomainSource.WORLDBANK, "pkb"))
        if not pkb_df or pkb_df.count() == 0:
            raise ValueError("No data found in the 'pkb' data frame.")

        # 3. Normalize the GDP data columns
        # As per the WorldBankTransformer, the input data should contain 'ISO3166-1-Alpha-3', 'year', and 'value'.
        pkb_df = pkb_df.select(
            F.col("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value")
        )

        # 4. Join with the country model on ISO
        final_df = pkb_df.join(
            country_df,
            pkb_df["ISO3166-1-Alpha-3"] == country_df["ISO3166-1-Alpha-3"],
            "inner"  # Inner join returns only countries from the reference list.
        ).select(
            pkb_df["ISO3166-1-Alpha-3"].alias("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value"),
            F.col("country_name"),
            F.col("ref_worldbank")  # Flag indicating that the country exists in the World Bank data
        )

        return final_df