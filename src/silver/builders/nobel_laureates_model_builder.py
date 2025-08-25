# src/silver/builders/graduate_unemployment_model_builder.py
from injector import inject
import pyspark.sql.functions as F
from typing import Dict, Tuple
from pyspark.sql import DataFrame

from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.silver_model_builder import SilverModelBuilder


@ModelBuilderRegistry.register(ModelType.NOBEL_LAUREATES)
class NobelLaureatesModelBuilder(SilverModelBuilder):
    async def build(
        self,
        datasets: Dict[Tuple, DataFrame],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        return super().build(datasets)