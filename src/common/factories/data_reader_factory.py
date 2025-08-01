# src/common/factories/data_reader_factory.py

from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.registers.data_reader_registry import Data
from src.common.readers.base_data_reader import BaseDataReader
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.registers.data_reader_registry import DataReaderRegistry
from pyspark.sql import SparkSession
from typing import Type, Any, List



class DataReaderFactory(BaseFactoryFromRegistry[DomainSource, BaseDataReader]):
    """
    Fabryka do tworzenia instancji DataReaderów na podstawie ModelType.
    """
    @classmethod
    def get_registry(cls) -> BaseRegistry[DomainSource, Type[BaseDataReader]]:
        """
        Zwraca instancję DataReaderRegistry.
        """
        return DataReaderRegistry()