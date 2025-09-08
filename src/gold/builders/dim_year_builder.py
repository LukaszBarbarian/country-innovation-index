import datetime
from src.common.builders.analytical_builder import AnalyticalBaseBuilder
from src.common.enums.model_type import ModelType
from src.common.models.build_request import BuildRequest
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.common.registers.analytical_model_registry import AnalyticalModelRegistry

@AnalyticalModelRegistry.register("dim_year")
class DimYearBuilder(AnalyticalBaseBuilder):
    async def run(self, request: BuildRequest) -> DataFrame:

        start_year = 1901
        end_year = datetime.datetime.now().year

        df_years = self.spark_service.spark.range(1).select(
            F.explode(F.sequence(F.lit(start_year), F.lit(end_year))).alias("year")
        )

        # Dodajemy opcjonalny sztuczny klucz
        df_years = df_years.withColumn("year_id", F.monotonically_increasing_id())

        return df_years