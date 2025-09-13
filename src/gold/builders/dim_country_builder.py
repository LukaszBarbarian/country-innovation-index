from src.common.builders.analytical_builder import AnalyticalBaseBuilder
from src.common.enums.model_type import ModelType
from src.common.models.build_request import BuildRequest
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from src.common.registers.analytical_model_registry import AnalyticalModelRegistry

@AnalyticalModelRegistry.register("dim_country")
class DimCountryBuilder(AnalyticalBaseBuilder):
    """
    A builder class for creating the 'dim_country' analytical model.
    It retrieves the country data from the loaded DataFrames and returns it
    as the final model. This class is registered with `AnalyticalModelRegistry`
    under the name "dim_country".
    """
    async def run(self, request: BuildRequest) -> DataFrame:
        """
        Executes the build logic to create the 'dim_country' model.

        Args:
            request (BuildRequest): The request object containing the loaded DataFrames.

        Returns:
            DataFrame: A Spark DataFrame representing the 'dim_country' model.
        """
        loaded = request.loaded_dfs or {}

        return loaded.get(ModelType.COUNTRY)