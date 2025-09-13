# src/common/transformers/patents_transformer.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.registers.domain_transformer_registry import DomainTransformerRegistry
from src.common.transformators.base_transformer import BaseTransformer


@DomainTransformerRegistry.register(DomainSource.PATENTS)
class PatentsTransformer(BaseTransformer):
    """
    A transformer for patents data, registered to handle the `DomainSource.PATENTS` source.

    This class performs several key transformations on the raw patents data,
    including reshaping the data from a wide format to a long format, cleaning
    and casting data types, and creating new columns to categorize patents as
    'resident' or 'abroad'. It also prepares a column for joining with country data.
    """
    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Transforms the raw patents DataFrame.

        This method pivots the data from a "wide" format (with separate columns for each year)
        to a "long" format, making it suitable for analysis. It also adds new columns
        for resident and abroad patents.

        Args:
            df (DataFrame): The raw DataFrame containing patents data.
            dataset_name (str): The name of the dataset (e.g., 'patents').

        Returns:
            DataFrame: The transformed DataFrame in a long format.
        """
        if dataset_name == "patents":
            # Select columns representing years
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]

            # Stack the year columns into a "long" format
            df_long = df.selectExpr(
                "`Office`",
                "`Office (Code)`",
                "`Origin`",
                f"stack({len(year_columns)}, " + 
                ", ".join([f"'{year}', `{year}`" for year in year_columns]) + 
                ") as (year, patents_count_str)"
            )

            df_long = df_long.withColumn(
                "patents_count", F.expr("try_cast(patents_count_str as long)")
            ).filter(F.col("patents_count").isNotNull() & (F.col("patents_count") > 0))

            df_long = df_long.withColumn("year", F.col("year").cast("integer"))
            df_long = df_long.withColumn("patents_id", F.monotonically_increasing_id())
            df_long = df_long.drop("patents_count_str")

            # Categorize patents as resident or abroad
            df_long = df_long.withColumn(
                "resident_patents", F.when(F.col("Origin") == F.col("Office"), F.col("patents_count")).otherwise(0)
            ).withColumn(
                "abroad_patents", F.when(F.col("Origin") != F.col("Office"), F.col("patents_count")).otherwise(0)
            )

            # Create a column for country codes, useful for joins
            df_long = df_long.withColumn(
                "country_code",
                F.upper(F.trim(F.col("Origin")))
            )

            return df_long
        return df

    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Normalizes the DataFrame.

        The `country_code` is already created and normalized in the `transform` method,
        so no further normalization is needed here.
        """
        return df

    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Performs data enrichment, though no specific enrichment logic is implemented here.
        """
        return df