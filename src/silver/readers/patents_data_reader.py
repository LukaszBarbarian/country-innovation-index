# src/silver/data_readers/nobel_prize_data_reader.py
from pyspark.sql import DataFrame, SparkSession
from injector import inject
from src.common.readers.base_data_reader import BaseDataReader
from src.common.enums.domain_source import DomainSource
from src.common.registers.data_reader_registry import DataReaderRegistry
from typing import TypeVar


@DataReaderRegistry.register(DomainSource.PATENTS)
class PatentsDataReader(BaseDataReader):
    """
    """
    

    def _load_from_source(self, path):
        return super()._load_from_source(path)