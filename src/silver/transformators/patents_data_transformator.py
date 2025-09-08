# src/common/transformers/patents_transformer.py
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.registers.domain_transformer_registry import DomainTransformerRegistry
from src.common.transformators.base_transformer import BaseTransformer

@DomainTransformerRegistry.register(DomainSource.PATENTS)
class PatentsTransformer(BaseTransformer):
    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        if dataset_name == "patents":
            # Wybieramy kolumny lat
            year_columns = [col for col in df.columns if col.isdigit() and len(col) == 4]

            # Stackujemy lata w "długi" format
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

            # resident vs abroad
            df_long = df_long.withColumn(
                "resident_patents", F.when(F.col("Origin") == F.col("Office"), F.col("patents_count")).otherwise(0)
            ).withColumn(
                "abroad_patents", F.when(F.col("Origin") != F.col("Office"), F.col("patents_count")).otherwise(0)
            )

            # kolumna do joinów z krajami
            df_long = df_long.withColumn(
                "country_code",
                F.upper(F.trim(F.col("Origin")))
            )

            return df_long
        return df

    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        # Już mamy country_code w transform, więc tu nic nie trzeba
        return df

    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        return df