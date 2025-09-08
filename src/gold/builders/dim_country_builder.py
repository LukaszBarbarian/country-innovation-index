from src.common.builders.analytical_builder import AnalyticalBaseBuilder
from src.common.enums.model_type import ModelType
from src.common.models.build_request import BuildRequest
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from src.common.registers.analytical_model_registry import AnalyticalModelRegistry

@AnalyticalModelRegistry.register("dim_country")
class DimCountryBuilder(AnalyticalBaseBuilder):
    async def run(self, request: BuildRequest) -> DataFrame:
        loaded = request.loaded_dfs or {}

        return loaded.get(ModelType.COUNTRY)
