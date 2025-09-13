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
    """
    A builder class for creating a 'dim_year' (year dimension) analytical model.

    This builder generates a Spark DataFrame containing a list of years from 1901
    to the current year. It's intended to be used as a dimension table in a data warehouse
    for time-based analysis.
    """
    async def run(self, request: BuildRequest) -> DataFrame:
        """
        Executes the build logic to create the 'dim_year' model.

        The method generates a sequence of years and creates a DataFrame from it.
        It also adds a monotonically increasing ID as an optional surrogate key.

        Args:
            request (BuildRequest): The request object (not used directly in this builder,
                                    but required by the base class).

        Returns:
            DataFrame: A Spark DataFrame representing the 'dim_year' model with 'year' and
                       'year_id' columns.
        """
        start_year = 1901
        end_year = datetime.datetime.now().year

        df_years = self.spark_service.spark.range(1).select(
            F.explode(F.sequence(F.lit(start_year), F.lit(end_year))).alias("year")
        )

        # Add an optional surrogate key
        df_years = df_years.withColumn("year_id", F.monotonically_increasing_id())

        return df_years