# src/common/builders/base_model_builder.py

from pyspark.sql import DataFrame
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from injector import inject, Injector

from src.common.config.config_manager import ConfigManager
from src.common.enums.model_type import ModelType
from src.common.models.etl_model import EtlModel
from src.common.enums.domain_source import DomainSource
from src.common.models.base_context import BaseContext
from src.common.spark.spark_service import SparkService


class BaseModelBuilder(ABC):
    @inject
    def __init__(
        self,
        spark: SparkService,
        injector: Injector,
        context: BaseContext,
        config: ConfigManager
    ):
        self._spark = spark
        self._injector = injector
        self._context = context
        self._config = config
        self._model_type: Optional[ModelType] = None

    def set_identity(self, model_type: ModelType):
        self._model_type = model_type

    @abstractmethod
    def load_data(self) -> Dict[DomainSource, Dict[str, DataFrame]]:
        raise NotImplementedError

    @abstractmethod
    async def build(self, datasets: Dict[DomainSource, Dict[str, DataFrame]], 
                    dependencies: Dict[ModelType, DataFrame]) -> DataFrame:
        raise NotImplementedError

    def get_partition_cols(self) -> Optional[List[str]]:
        return None

    def create_model(self, df: DataFrame) -> EtlModel:
        """
        Domyślna implementacja tworzenia obiektu EtlModel.
        """
        metadata = {
            "ingestion_time_utc": self._context.ingestion_time_utc,
            "correlation_id": self._context.correlation_id,
        }
        return EtlModel(
            data=df,
            metadata=metadata,
            type=self._model_type,
            partition_cols=self.get_partition_cols()
        )
