from typing import Dict
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.types import DoubleType as DblType
from pyspark.ml.functions import vector_to_array

from src.common.enums.model_type import ModelType
from src.common.models.build_request import BuildRequest
from src.common.builders.analytical_builder import AnalyticalBaseBuilder
from src.common.registers.analytical_model_registry import AnalyticalModelRegistry


@AnalyticalModelRegistry.register("fact_innovation")
class FactInnovationBuilder(AnalyticalBaseBuilder):
    """
    A builder class for creating the 'fact_innovation' analytical model.
    This class orchestrates a multi-step process to generate an "Innovation Score"
    for countries across different years by joining various data sources, calculating
    derived metrics, and normalizing the data.
    """
    async def run(self, request: BuildRequest) -> DataFrame:
        """
        Executes the build logic to create the 'fact_innovation' model.

        The process involves:
        1. Joining all necessary DataFrames (countries, population, GDP, etc.) on `ISO3166-1-Alpha-3` and `year`.
        2. Aggregating patent and Nobel laureate data.
        3. Calculating derived metrics like patents per million, Nobelists per million, and R&D as a percentage of GDP.
        4. Normalizing the metrics using Spark's `MinMaxScaler` to a range of [0, 1].
        5. Calculating the final `innovation_score` as a weighted sum of the normalized metrics.
        6. Returning a clean DataFrame with the final score and original columns.

        Args:
            request (BuildRequest): The request object containing all loaded DataFrames.

        Returns:
            DataFrame: A Spark DataFrame representing the 'fact_innovation' model.
        """
        loaded = request.loaded_dfs or {}

        # 🔹 Weights (intended to be from a config file)
        w1, w2, w3, w4, w5, w6, w7 = 0.25, 0.25, 0.15, 0.15, 0.1, 0.1, 0.1

        df_country = loaded.get(ModelType.COUNTRY)
        df_population = loaded.get(ModelType.POPULATION)
        df_pkb = loaded.get(ModelType.PKB)
        df_rd = loaded.get(ModelType.RD_EXPENDITURE)
        df_researchers = loaded.get(ModelType.RESEARCHERS)
        df_unemp = loaded.get(ModelType.GRADUATE_UNEMPLOYMENT)
        df_patents = loaded.get(ModelType.PATENTS)
        df_nobels = loaded.get(ModelType.NOBEL_LAUREATES)
        df_year = loaded.get(ModelType.YEAR)

        # 🔹 Cross join country × year
        df_country_year = df_country.crossJoin(df_year)

        # 🔹 Aggregate patents (by country and year)
        df_patents_agg = (
            df_patents.groupBy("ISO3166-1-Alpha-3", "year")
            .agg(
                F.sum("patents_total").alias("patents_total"),
                F.sum("resident_patents").alias("resident_patents"),
                F.sum("abroad_patents").alias("abroad_patents")
            )
        )

        # 🔹 Join all sources
        df = (
            df_country_year
            .join(df_population.select("ISO3166-1-Alpha-3", "year",
                                       F.col("value").alias("population")),
                  ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_pkb.select("ISO3166-1-Alpha-3", "year",
                                F.col("value").alias("gdp")),
                  ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_rd.select("ISO3166-1-Alpha-3", "year",
                               F.col("value").alias("rd_expenditure")),
                  ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_researchers.select("ISO3166-1-Alpha-3", "year",
                                        F.col("value").alias("researchers_count")),
                  ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_unemp.select("ISO3166-1-Alpha-3", "year",
                                  F.col("unemployment_rate").alias("graduate_unemployment_rate")),
                  ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_patents_agg,
                  ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_nobels.groupBy("ISO3166-1-Alpha-3", "year")
                           .agg(F.countDistinct("laureate_id").alias("nobel_laureates_count")),
                  ["ISO3166-1-Alpha-3", "year"], "left")
        )

        # 🔹 Derived metrics (protected against division by zero)
        df = df.withColumn(
            "patents_per_million",
            F.when(F.col("population").isNotNull() & (F.col("population") > 0),
                   F.col("patents_total").cast(DblType()) / (F.col("population").cast(DblType()) / 1_000_000)).otherwise(F.lit(0))
        ).withColumn(
            "resident_patents_per_million",
            F.when(F.col("population").isNotNull() & (F.col("population") > 0),
                   F.col("resident_patents").cast(DblType()) / (F.col("population").cast(DblType()) / 1_000_000)).otherwise(F.lit(0))
        ).withColumn(
            "nobelists_per_million",
            F.when(F.col("population").isNotNull() & (F.col("population") > 0),
                   F.col("nobel_laureates_count").cast(DblType()) / (F.col("population").cast(DblType()) / 1_000_000)).otherwise(F.lit(0))
        ).withColumn(
            "patent_expansion_ratio",
            F.when((F.col("resident_patents") + F.lit(1)).isNotNull() & ((F.col("resident_patents") + F.lit(1)) > 0),
                   F.col("abroad_patents").cast(DblType()) / (F.col("resident_patents").cast(DblType()) + F.lit(1))).otherwise(F.lit(0))
        ).withColumn(
            "researchers_per_million",
            F.when(F.col("population").isNotNull() & (F.col("population") > 0),
                   F.col("researchers_count").cast(DblType()) / (F.col("population").cast(DblType()) / 1_000_000)).otherwise(F.lit(0))
        ).withColumn(
            "rd_gdp_pct",
            F.when(F.col("gdp").isNotNull() & (F.col("gdp") > 0),
                   (F.col("rd_expenditure").cast(DblType()) / F.col("gdp").cast(DblType())) * 100).otherwise(F.lit(0))
        )
        
        # 🔹 Data Normalization
        cols_to_normalize = [
            "patents_per_million",
            "resident_patents_per_million",
            "nobelists_per_million",
            "patent_expansion_ratio",
            "researchers_per_million",
            "rd_gdp_pct",
            "graduate_unemployment_rate"
        ]
        
        # Replace nulls with 0, then assemble vectors.
        df_clean = df.fillna(0, subset=cols_to_normalize)
        assembler = VectorAssembler(inputCols=cols_to_normalize, outputCol="features")
        df_assembled = assembler.transform(df_clean)

        # Use MinMaxScaler to scale all columns to the [0, 1] range.
        scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        # 🔹 Convert vector to array using Spark's built-in function
        df_scaled_array = df_scaled.withColumn(
            "scaled_array", vector_to_array(F.col("scaled_features"))
        )
        
        # 🔹 Calculate Innovation Score based on the normalized array
        df_final = df_scaled_array.withColumn(
            "innovation_score",
            F.col("scaled_array").getItem(0) * w1 +
            F.col("scaled_array").getItem(1) * w2 +
            F.col("scaled_array").getItem(2) * w6 +
            F.col("scaled_array").getItem(3) * w3 +
            F.col("scaled_array").getItem(4) * w4 +
            F.col("scaled_array").getItem(5) * w5 -
            F.col("scaled_array").getItem(6) * w7
        )

        # Remove temporary columns and return the final DataFrame.
        final_cols = [c for c in df.columns] + ["innovation_score"]
        
        return df_final.select(*final_cols)