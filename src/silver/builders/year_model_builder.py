# src/silver/model_builders/years_model_builder.py
import datetime
from typing import Dict, Tuple
from injector import Injector, inject
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from src.common.config.config_manager import ConfigManager
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.common.spark.spark_service import SparkService
from src.silver.builders.silver_model_builder import SilverModelBuilder
from src.silver.context.silver_context import SilverContext
from src.silver.readers.manual_data_reader import ManualDataReader
from src.silver.readers.reference_data_reader import ReferenceDataReader


@ModelBuilderRegistry.register(ModelType.YEAR)
class YearsModelBuilder(SilverModelBuilder):
    @inject
    def __init__(
        self,
        spark_service: SparkService,
        injector: Injector,
        context: SilverContext,
        config: ConfigManager,
        manual_data_reader: ManualDataReader,
        reference_data_reader: ReferenceDataReader
    ):
        super().__init__(spark_service, injector, context, config, manual_data_reader, reference_data_reader)
        self.synthetic = True

    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        start_year = 1901
        end_year = datetime.datetime.now().year

        df_years = self._spark_service.spark.range(1).select(
            F.explode(F.sequence(F.lit(start_year), F.lit(end_year))).alias("year")
        )

        # Dodajemy opcjonalny sztuczny klucz
        df_years = df_years.withColumn("year_id", F.monotonically_increasing_id())

        return df_years
