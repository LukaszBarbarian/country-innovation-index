# src/common/readers/base_data_reader.py
from abc import ABC, abstractmethod
from asyncio.log import logger
from pyspark.sql import DataFrame
from src.common.config.config_manager import ConfigManager
from src.common.enums.domain_source import DomainSource
from src.common.models.base_context import BaseContext
from injector import inject
from typing import Dict, List, Optional
import asyncio # Ważne: import asyncio do użycia w klasach potomnych

from src.common.models.ingestion_result import IngestionResult
from src.common.spark.spark_service import SparkService

class BaseDataReader(ABC):
    """
    Bazowa klasa abstrakcyjna dla wszystkich czytników danych.
    Zapewnia automatyczne zarządzanie cache'owaniem danych.
    """
    @inject
    def __init__(self, spark: SparkService, context: BaseContext, config: ConfigManager):
        self._spark = spark
        self._context = context
        self._config = config
        self._loaded_dataframes: Dict[str, DataFrame] = {}

    def set_domain_source(self, domain_source: DomainSource):
        self._domain_source = domain_source

    def load_data(self, dataset_names: Optional[List[str]] = None) -> Dict[str, DataFrame]:
        """
        Główna metoda do odczytu danych.
        Ładuje tylko określone datasety lub wszystkie dostępne,
        jeśli 'dataset_names' nie jest ustawione.
        Zarządza również cache'owaniem.
        """
        if not self._domain_source:
            logger.error("Domain source not set. Cannot load data.")
            return {}
        
        cache_key = self._domain_source.name
        
        if self._context._cache.exists(cache_key):
            cached_data = self._context._cache.get(cache_key)
            logger.info(f"[{self._domain_source.name}] Retrieving data from cache.")
            
            if dataset_names:
                return {name: df for name, df in cached_data.items() if name in dataset_names}
            
            return cached_data
        
        logger.info(f"[{self._domain_source.name}] Loading data from source.")

        ingestion_results = self._context.get_ingestion_result(self._domain_source)
        
        if not ingestion_results:
            logger.warning(f"No ingestion results found for domain source '{self._domain_source.name}'.")
            return {}

        all_dataframes = self._load_from_source(ingestion_results)
        
        if all_dataframes:
            self._context._cache.set(cache_key, all_dataframes)
            
            if dataset_names:
                return {name: df for name, df in all_dataframes.items() if name in dataset_names}
        
        return all_dataframes or {}
    


    @abstractmethod
    def _load_from_source(self, all_readers: Optional[List[IngestionResult]]) -> Dict[str, DataFrame]:
        """
        Abstrakcyjna metoda, która powinna zawierać logikę faktycznego
        ładowania danych z pliku i zwracać słownik DataFrame'ów.
        """
        pass