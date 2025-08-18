# src/silver/model_builders/base_model_builder.py
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod
from typing import Dict, cast
from injector import inject, Injector

from src.common.config.config_manager import ConfigManager
from src.common.enums.model_type import ModelType
from src.common.readers.base_data_reader import BaseDataReader
from src.common.factories.data_reader_factory import DataReaderFactory
from src.common.enums.domain_source import DomainSource
from src.common.models.base_context import BaseContext
from src.common.spark.spark_service import SparkService

# Importuj swoją nową klasę BaseModel
from src.common.models.etl_model import EtlModel


class BaseModelBuilder(ABC):
    @inject
    def __init__(self, spark: SparkService, 
                 injector: Injector, 
                 context: BaseContext,
                 config: ConfigManager):
        
        self._spark = spark
        self._injector = injector
        self._context = context
        self._config = config

    def set_identity(self, model_type: ModelType):
        self._model_type = model_type

    def get_reader(self, domain_source: DomainSource) -> BaseDataReader:
        reader_class = DataReaderFactory.get_class(domain_source)
        if not issubclass(reader_class, BaseDataReader):
            raise TypeError(f"Reader class for {domain_source} must be a subclass of BaseDataReader.")
        
        reader = self._injector.get(reader_class)
        reader.set_domain_source(domain_source)
        return reader
    

    
    
   
    async def run(self) -> EtlModel:
        dataframes_dict = await self._load_data()

        if dataframes_dict:
            processed_dfs = {}
            for dataset_name, df in dataframes_dict.items():
                processed_df = await self.transform(df, dataset_name)
                processed_df = await self.normalize(processed_df, dataset_name)
                processed_df = await self.enrich(processed_df, dataset_name)
                processed_dfs[dataset_name] = processed_df
            
            final_df = await self.combine(processed_dfs)
        
        return self._create_model(final_df)



    @abstractmethod
    async def _load_data(self) -> DataFrame: # <-- Zmieniona nazwa
        """Abstrakcyjna metoda do ładowania surowego DataFrame."""
        raise NotImplementedError



    def _create_model(self, df: DataFrame) -> EtlModel:
        """Tworzy obiekt BaseModel z przetworzonego DataFrame."""
        metadata = {
            "ingestion_time_utc": self._context.ingestion_time_utc,
            "correlation_id": self._context.correlation_id,
        }
        return EtlModel(data=df, metadata=metadata, model_type=self._model_type)
    



    @abstractmethod
    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        raise NotImplementedError
        
    @abstractmethod
    async def combine(self, dataframes: Dict[str, DataFrame]) -> DataFrame:
        """Łączy wszystkie przetworzone DataFrames w jeden finalny."""
        raise NotImplementedError