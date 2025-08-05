# src/silver/data_readers/nobel_prize_data_reader.py
from pyspark.sql import DataFrame, SparkSession
from injector import inject
from src.common.readers.base_data_reader import BaseDataReader
from src.common.enums.domain_source import DomainSource
from src.silver.contexts.silver_context import SilverContext
from src.common.registers.data_reader_registry import DataReaderRegistry
from typing import TypeVar


@DataReaderRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeDataReader(BaseDataReader):
    """
    Czytnik danych dla nagród Nobla, pobierający dane z warstwy Bronze.
    Wczytuje plik Parquet z lokalizacji podanej w kontekście.
    """
    

    def _load_from_source(self):
        try:
            file_path = "abfss://bronze@demosurdevdatalake4418sa.dfs.core.windows.net/NOBELPRIZE/2025/08/04/nobelPrizes_736984ab-3b21-4c9f-b2dd-7863f1a74389_910378ab.json"

            df = self._spark.read.option("multiLine", "true").json(file_path)
            return df
        except Exception as e:
            raise IOError(f"Błąd odczytu pliku Parquet z lokalizacji {file_path}. Szczegóły: {e}")
