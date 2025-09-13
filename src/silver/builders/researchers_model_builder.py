from typing import Dict, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.RESEARCHERS)
class ResearchersModelBuilder(SilverModelBuilder):
    """
    A model builder for the 'researchers' model in the Silver layer.
    
    This builder takes raw data on researchers from the World Bank and enriches it by joining
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
        Builds the 'researchers' model by joining researchers data with the country dimension table.

        Args:
            datasets (Dict[Tuple, DataFrame]): A dictionary of loaded DataFrames from various sources.
            dependencies (Dict[ModelType, DataFrame]): A dictionary of DataFrames from other Silver layer models.

        Returns:
            DataFrame: An enriched Spark DataFrame for researchers by country and year.

        Raises:
            ValueError: If the 'COUNTRY' model or 'researchers' dataset is missing.
        """
        country_df = dependencies.get(ModelType.COUNTRY)
        if country_df is None or country_df.count() == 0:
            raise ValueError("The country model (COUNTRY) is missing from dependencies.")

        # 2. Get the researchers data frame from datasets
        researchers_df = datasets.get((DomainSource.WORLDBANK, "researchers"))
        if not researchers_df or researchers_df.count() == 0:
            raise ValueError("No data found in the 'researchers' data frame.")

        # 3. Normalize the researchers data columns
        # As per the WorldBankTransformer, the input data should contain 'ISO3166-1-Alpha-3', 'year', and 'value'.
        researchers_df = researchers_df.select(
            F.col("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value")
        )

        # 4. Join with the country model on ISO
        final_df = researchers_df.join(
            country_df,
            researchers_df["ISO3166-1-Alpha-3"] == country_df["ISO3166-1-Alpha-3"],
            "inner"  # Inner join returns only countries from the reference list.
        ).select(
            researchers_df["ISO3166-1-Alpha-3"].alias("ISO3166-1-Alpha-3"),
            F.col("year"),
            F.col("value"),
            F.col("country_name"),
            F.col("ref_worldbank")  # Flag indicating that the country exists in the World Bank data
        )

        return final_df