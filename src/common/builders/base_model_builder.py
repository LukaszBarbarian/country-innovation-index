# src/silver/model_builders/base_model_builder.py (bez zmian w tej klasie względem ostatniej propozycji)
from pyspark.sql import SparkSession
from abc import ABC, abstractmethod
from injector import inject, Injector
from src.common.config.config_manager import ConfigManager
from src.common.readers.base_data_reader import BaseDataReader
from src.common.factories.data_reader_factory import DataReaderFactory
from src.common.enums.domain_source import DomainSource
from src.common.models.base_model import BaseModel
from src.common.contexts.base_layer_context import BaseLayerContext


class BaseModelBuilder(ABC):
    @inject
    def __init__(self, spark: SparkSession, 
                 injector: Injector, 
                 context: BaseLayerContext,
                 config: ConfigManager):
        
        self._spark = spark
        self._injector = injector
        self._context = context
        self._config = config

    def get_reader(self, domain_source: DomainSource) -> BaseDataReader:
        reader_class = DataReaderFactory.get_class(domain_source)
        
        if not issubclass(reader_class, BaseDataReader):
            raise TypeError(f"Reader class for {domain_source} must be a subclass of BaseDataReader.")

        reader = self._injector.get(reader_class)
        reader.set_domain_source(domain_source)

        return reader

    
    async def run(self) -> BaseModel:
        model = await self.build()
        model = await self.normalize(model)
        model = await self.enrich(model)
        model = await self.transform(model)

        return model
    



    @abstractmethod
    async def build(self) -> BaseModel:
        raise NotImplementedError


    @abstractmethod
    async def normalize(self, model: BaseModel) -> BaseModel:
        """Normalizacja danych."""
        raise NotImplementedError

    @abstractmethod
    async def enrich(self, model: BaseModel) -> BaseModel:
        """Wzbogacanie danych."""
        raise NotImplementedError

    @abstractmethod
    async def transform(self, model: BaseModel) -> BaseModel:
        """Transformacja danych."""
        raise NotImplementedError
