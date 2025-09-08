# src/common/builders/base_model_builder.py
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from injector import inject, Injector

from src.common.config.config_manager import ConfigManager
from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.models.base_context import ContextBase
from src.common.models.base_process import BaseProcessModel
from src.common.spark.spark_service import SparkService


class BaseModelBuilder(ABC):
    @inject
    def __init__(
        self,
        spark_service: SparkService,
        injector: Injector,
        context: ContextBase,
        config: ConfigManager
    ):
        self._spark_service = spark_service
        self._injector = injector
        self._context = context
        self._config = config
        self._model_type: Optional[ModelType] = None
        self.synthetic = False

    def set_identity(self, model_type: ModelType):
        self._model_type = model_type

    @abstractmethod
    def load_data(self) -> Dict[DomainSource, Dict[str, DataFrame]]:
        """Ładuje dane źródłowe dla modelu."""
        raise NotImplementedError

    @abstractmethod
    async def build(
        self,
        datasets: Dict[DomainSource, Dict[str, DataFrame]],
        dependencies: Dict[ModelType, DataFrame]
    ) -> DataFrame:
        """Tworzy DataFrame modelu na podstawie przetworzonych danych i zależności."""
        raise NotImplementedError

    def get_partition_cols(self) -> Optional[List[str]]:
        return None


    @abstractmethod
    def create_model(self, df: DataFrame) -> BaseProcessModel:
        raise NotImplementedError
