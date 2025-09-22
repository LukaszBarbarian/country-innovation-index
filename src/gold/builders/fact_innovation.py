from typing import Dict
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType as DblType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.window import Window 

# Importy klas z Twojego projektu
from src.common.enums.model_type import ModelType
from src.common.models.build_request import BuildRequest
from src.common.builders.analytical_builder import AnalyticalBaseBuilder
from src.common.registers.analytical_model_registry import AnalyticalModelRegistry


@AnalyticalModelRegistry.register("fact_innovation")
class FactInnovationBuilder(AnalyticalBaseBuilder):
    async def run(self, request: BuildRequest) -> DataFrame:
        loaded = request.loaded_dfs or {}

        # 🔹 Wagi podindeksów i ich składowych
        weights = {
            "patents_index": 0.35,
            "research_index": 0.35,
            "rd_index": 0.2,
            "unemployment_index": 0.1,
        }

        subindex_weights = {
            "patents_index": {
                "patents_per_million_norm": 0.4,
                "resident_patents_per_million_norm": 0.3,
                "patent_expansion_ratio_norm": 0.3,
            },
            "research_index": {
                "researchers_per_million_norm": 0.6,
                "nobelists_per_million_norm": 0.4,
            },
            "rd_index": {
                "rd_gdp_pct_norm": 1.0,
            },
            "unemployment_index": {
                "graduate_unemployment_rate_norm": 1.0,
            },
        }

        # 🔹 Ładowanie i łączenie danych
        df_country = loaded.get(ModelType.COUNTRY)
        df_population = loaded.get(ModelType.POPULATION)
        df_pkb = loaded.get(ModelType.PKB)
        df_rd = loaded.get(ModelType.RD_EXPENDITURE)
        df_researchers = loaded.get(ModelType.RESEARCHERS)
        df_unemp = loaded.get(ModelType.GRADUATE_UNEMPLOYMENT)
        df_patents = loaded.get(ModelType.PATENTS)
        df_nobels = loaded.get(ModelType.NOBEL_LAUREATES)
        df_year = loaded.get(ModelType.YEAR)

        df_country_iso = df_country.select("ISO3166-1-Alpha-3")
        df_year_col = df_year.select("year")
        df_country_year = df_country_iso.crossJoin(df_year_col)

        df_patents_agg = (
            df_patents.groupBy("ISO3166-1-Alpha-3", "year")
            .agg(
                F.sum("patents_total").alias("patents_total"),
                F.sum("resident_patents").alias("resident_patents"),
                F.sum("abroad_patents").alias("abroad_patents"),
            )
        )

        df = (
            df_country_year
            .join(df_population.select("ISO3166-1-Alpha-3", "year", F.col("value").alias("population")), ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_pkb.select("ISO3166-1-Alpha-3", "year", F.col("value").alias("gdp")), ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_rd.select("ISO3166-1-Alpha-3", "year", F.col("value").alias("rd_expenditure")), ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_researchers.select("ISO3166-1-Alpha-3", "year", F.col("value").alias("researchers_count")), ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_unemp.select("ISO3166-1-Alpha-3", "year", F.col("unemployment_rate").alias("graduate_unemployment_rate")), ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_patents_agg, ["ISO3166-1-Alpha-3", "year"], "left")
            .join(df_nobels.groupBy("ISO3166-1-Alpha-3", "year").agg(F.countDistinct("laureate_id").alias("nobel_laureates_count")), ["ISO3166-1-Alpha-3", "year"], "left")
        )

        # 🔹 Obliczenia metryk pochodnych
        df = df.withColumn(
            "patents_per_million",
            F.when(F.col("population") > 0, F.col("patents_total").cast(DblType()) / (F.col("population") / 1_000_000)).otherwise(F.lit(None))
        ).withColumn(
            "resident_patents_per_million",
            F.when(F.col("population") > 0, F.col("resident_patents").cast(DblType()) / (F.col("population") / 1_000_000)).otherwise(F.lit(None))
        ).withColumn(
            "nobelists_per_million",
            F.when(F.col("population") > 0, F.col("nobel_laureates_count").cast(DblType()) / (F.col("population") / 1_000_000)).otherwise(F.lit(None))
        ).withColumn(
            "patent_expansion_ratio",
            F.when(F.col("resident_patents") > 0, F.col("abroad_patents").cast(DblType()) / F.col("resident_patents")).otherwise(F.lit(None))
        ).withColumn(
            "researchers_per_million",
            F.when(F.col("population") > 0, F.col("researchers_count").cast(DblType()) / (F.col("population") / 1_000_000)).otherwise(F.lit(None))
        ).withColumn(
            "rd_gdp_pct",
            F.when(F.col("gdp") > 0, (F.col("rd_expenditure").cast(DblType()) / F.col("gdp")) * 100).otherwise(F.lit(None))
        )
        
        # 🔹 Lista kolumn do normalizacji
        norm_cols = [
            "patents_per_million",
            "resident_patents_per_million",
            "nobelists_per_million",
            "patent_expansion_ratio",
            "researchers_per_million",
            "rd_gdp_pct",
            "graduate_unemployment_rate",
        ]
        
        # 🔹 Log transform
        temp_df = df.fillna(0, subset=norm_cols)
        log_cols = [f"{c}_log" for c in norm_cols]
        for c in norm_cols:
            temp_df = temp_df.withColumn(f"{c}_log", F.log1p(F.col(c)))

        # 🔹 Normalizacja
        assembler = VectorAssembler(inputCols=log_cols, outputCol="features")
        temp_df_assembled = assembler.transform(temp_df)
        scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(temp_df_assembled)
        df_scaled = scaler_model.transform(temp_df_assembled)
        
        # 🔹 Kluczowa zmiana: Łączenie znormalizowanych danych z główną ramką
        df_scaled_with_array = df_scaled.withColumn("scaled_array", vector_to_array(F.col("scaled_features")))
        df = df.join(df_scaled_with_array.select(
            "ISO3166-1-Alpha-3",
            "year",
            "scaled_array"
        ), on=["ISO3166-1-Alpha-3", "year"], how="left")
        
        # 🔹 Obliczanie znormalizowanych kolumn
        for i, c in enumerate(norm_cols):
            df = df.withColumn(
                f"{c}_norm",
                F.when(
                    F.col(c).isNull(), F.lit(None)
                ).otherwise(
                    F.col("scaled_array").getItem(i)
                )
            )

        # 🔹 Usuwamy tymczasowe kolumny
        df = df.drop("scaled_array")
        
        # 🔹 Obliczanie podindeksów
        df = df.withColumn(
            "patents_index",
            F.when(
                (F.col("patents_per_million_norm").isNotNull() |
                 F.col("resident_patents_per_million_norm").isNotNull() |
                 F.col("patent_expansion_ratio_norm").isNotNull()),
                (F.coalesce(F.col("patents_per_million_norm"), F.lit(0)) * subindex_weights["patents_index"]["patents_per_million_norm"] +
                 F.coalesce(F.col("resident_patents_per_million_norm"), F.lit(0)) * subindex_weights["patents_index"]["resident_patents_per_million_norm"] +
                 F.coalesce(F.col("patent_expansion_ratio_norm"), F.lit(0)) * subindex_weights["patents_index"]["patent_expansion_ratio_norm"])
            ).otherwise(F.lit(None))
        ).withColumn(
            "research_index",
            F.when(
                (F.col("researchers_per_million_norm").isNotNull() |
                 F.col("nobelists_per_million_norm").isNotNull()),
                (F.coalesce(F.col("researchers_per_million_norm"), F.lit(0)) * subindex_weights["research_index"]["researchers_per_million_norm"] +
                 F.coalesce(F.col("nobelists_per_million_norm"), F.lit(0)) * subindex_weights["research_index"]["nobelists_per_million_norm"])
            ).otherwise(F.lit(None))
        ).withColumn(
            "rd_index",
            F.when(F.col("rd_gdp_pct_norm").isNotNull(), F.col("rd_gdp_pct_norm")).otherwise(F.lit(None))
        ).withColumn(
            "unemployment_index",
            F.when(F.col("graduate_unemployment_rate_norm").isNotNull(), 1 - F.col("graduate_unemployment_rate_norm")).otherwise(F.lit(None))
        )

        # 🔹 Obliczanie dynamicznej sumy wag i wartości
        sum_of_values_expr = (
            F.coalesce(F.col("patents_index"), F.lit(0)) * weights["patents_index"] +
            F.coalesce(F.col("research_index"), F.lit(0)) * weights["research_index"] +
            F.coalesce(F.col("rd_index"), F.lit(0)) * weights["rd_index"] +
            F.coalesce(F.col("unemployment_index"), F.lit(0)) * weights["unemployment_index"]
        )

        sum_of_weights_expr = (
            F.when(F.col("patents_index").isNotNull(), weights["patents_index"]).otherwise(0) +
            F.when(F.col("research_index").isNotNull(), weights["research_index"]).otherwise(0) +
            F.when(F.col("rd_index").isNotNull(), weights["rd_index"]).otherwise(0) +
            F.when(F.col("unemployment_index").isNotNull(), weights["unemployment_index"]).otherwise(0)
        )

        # 🔹 Finalny wynik
        df = df.withColumn(
            "innovation_score_final",
            F.when(
                sum_of_weights_expr > 0,
                sum_of_values_expr / sum_of_weights_expr
            ).otherwise(F.lit(None))
        )

        return df