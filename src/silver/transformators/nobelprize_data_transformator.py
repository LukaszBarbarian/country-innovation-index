import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.registers.domain_transformer_registry import DomainTransformerRegistry
from src.common.transformators.base_transformer import BaseTransformer


@DomainTransformerRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeTransformer(BaseTransformer):
    """
    A transformer for Nobel Prize data.

    This class handles the transformation and normalization of raw Nobel Prize
    laureates data from the Bronze layer, preparing it for the Silver layer.
    """
    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Transforms the raw Nobel Prize data by exploding nested structures and selecting
        relevant columns.

        Args:
            df (DataFrame): The raw DataFrame to transform.
            dataset_name (str): The name of the dataset (e.g., 'laureates').

        Returns:
            DataFrame: The transformed DataFrame with selected and flattened columns.
        """
        if dataset_name == "laureates":
            # Explode the array of prizes
            df = df.withColumn("prize", F.explode(F.col("nobelPrizes")))

            # Select only the required columns
            df = df.select(
                F.col("id").alias("laureate_id"),
                F.coalesce(F.col("birth.place.country.en"), 
                           F.col("birth.place.countryNow.en")).alias("country"),
                F.col("prize.awardYear").alias("year")
            )

            # Filter out records with null countries
            df = df.filter(F.col("country").isNotNull())

            return df

        return df

    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Normalizes the 'country' column by converting it to uppercase and trimming whitespace.

        Args:
            df (DataFrame): The DataFrame to normalize.
            dataset_name (str): The name of the dataset.

        Returns:
            DataFrame: The DataFrame with the new `country_normalized` column.
        """
        if "country" in df.columns:
            return df.withColumn(
                "country_normalized",
                F.upper(F.trim(F.col("country")))
            )
        return df

    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Performs data enrichment, though no specific enrichment logic is implemented here.
        
        Args:
            df (DataFrame): The DataFrame to enrich.
            dataset_name (str): The name of the dataset.

        Returns:
            DataFrame: The DataFrame, unchanged in this implementation.
        """
        return df