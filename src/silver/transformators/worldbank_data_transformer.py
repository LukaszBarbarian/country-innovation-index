import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.registers.domain_transformer_registry import DomainTransformerRegistry
from src.common.transformators.base_transformer import BaseTransformer


@DomainTransformerRegistry.register(DomainSource.WORLDBANK)
class WorldBankTransformer(BaseTransformer):
    """
    A transformer for World Bank data, which handles the transformation of various
    World Bank datasets from their raw JSON format into a more structured
    and consistent format.
    """

    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Transforms WB data by selecting only the country, ISO code, year, and indicator value.
        
        Args:
            df (DataFrame): The raw Spark DataFrame from the World Bank data source.
            dataset_name (str): The specific name of the dataset (e.g., "population", "unemployment").
            
        Returns:
            DataFrame: The transformed DataFrame with a simplified and consistent schema.
        """
        # All WB datasets have a similar structure
        value_column_map = {
            "population": "value",
            "unemployment": "unemployment_rate",
            "researchers": "value",  # Can be more specific later
            "rd": "value",
            "pkb": "value"
        }

        if dataset_name not in value_column_map:
            print(f"Warning: No transformation defined for dataset '{dataset_name}'. Returning original dataframe.")
            return df

        value_col = value_column_map[dataset_name]

        # Transformation: country, ISO code, year, value
        df = df.select(
            F.col("country.value").alias("country_name"),
            F.col("countryiso3code").alias("ISO3166-1-Alpha-3"),
            F.col("date").alias("year"),
            F.col("value").alias(value_col)
        )

        # Filter out records with missing ISO codes
        df = df.filter(F.col("ISO3166-1-Alpha-3").isNotNull())

        return df

    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Normalizes the 'country_name' column to a cleaner format suitable for joins.
        
        It converts the country name to uppercase, trims whitespace, and removes
        non-alphabetic characters.

        Args:
            df (DataFrame): The DataFrame to normalize.
            dataset_name (str): The name of the dataset.

        Returns:
            DataFrame: The DataFrame with the added `country_name_normalized` column.
        """
        if "country_name" in df.columns:
            return df.withColumn(
                "country_name_normalized",
                F.upper(F.trim(F.regexp_replace(F.col("country_name"), r"[^a-zA-Z\s]+", "")))
            )
        return df

    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Performs data enrichment, though no specific enrichment logic is implemented here.
        """
        return df