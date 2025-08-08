# src/common/readers/base_data_reader.py
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from src.common.config.config_manager import ConfigManager
from src.common.enums.domain_source import DomainSource
from src.common.contexts.base_layer_context import BaseLayerContext
from injector import inject

class BaseDataReader(ABC):
    """
    Bazowa klasa abstrakcyjna dla wszystkich czytników danych.
    Zapewnia automatyczne zarządzanie cache'owaniem danych.
    """
    @inject
    def __init__(self, spark: SparkSession, context: BaseLayerContext, config: ConfigManager):
        self._spark = spark
        self._context = context
        self._config = config

    def set_domain_source(self, domain_source: DomainSource):
        self._domain_source = domain_source

    def load_data(self) -> DataFrame:
        """
        Główna metoda do odczytu danych, która zarządza cache'owaniem.
        Sprawdza, czy dane są już w pamięci podręcznej kontekstu.
        """
        cache_key = self._domain_source.name

        if self._context._cache.exists(cache_key):
            print(f"[{self._domain_source.name}] Pobieram dane z cache'u.")
            return self._context._cache.get(cache_key)
        
        print(f"[{self._domain_source.name}] Ładuję dane z pliku i zapisuję w cache'u.")
        df = self._load_from_source()
        self._context._cache.set(cache_key, df)
        return df

    @abstractmethod
    def _load_from_source(self) -> DataFrame:
        """
        Abstrakcyjna metoda, która powinna zawierać logikę faktycznego
        ładowania danych z pliku.
        """
        pass