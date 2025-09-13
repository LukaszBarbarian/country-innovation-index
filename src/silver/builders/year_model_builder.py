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
    """
    A model builder for the 'years' model in the Silver layer.
    
    This builder is responsible for generating a synthetic dimension table
    of years. This model does not rely on external data sources and is built
    from scratch.
    """
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
        """
        Initializes the YearsModelBuilder. It sets the `synthetic` flag to True
        as this model is not based on external raw data.
        """
        super().__init__(spark_service, injector, context, config, manual_data_reader, reference_data_reader)
        self.synthetic = True

    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        """
        Builds the 'years' DataFrame.

        This method generates a sequence of years from 1901 to the current year
        and creates a Spark DataFrame from it.

        Args:
            datasets (Dict[Tuple, DataFrame]): Not used as this is a synthetic model.
            dependencies (Dict[ModelType, DataFrame]): Not used as this model has no dependencies.

        Returns:
            DataFrame: A Spark DataFrame containing a column of years and a
                       monotonically increasing ID.
        """
        start_year = 1901
        end_year = datetime.datetime.now().year

        df_years = self._spark_service.spark.range(1).select(
            F.explode(F.sequence(F.lit(start_year), F.lit(end_year))).alias("year")
        )

        # Add an optional synthetic key
        df_years = df_years.withColumn("year_id", F.monotonically_increasing_id())

        return df_years