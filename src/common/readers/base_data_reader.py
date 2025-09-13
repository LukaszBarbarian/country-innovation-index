# src/common/readers/base_data_reader.py
from abc import ABC, abstractmethod
from asyncio.log import logger
from pyspark.sql import DataFrame
from src.common.config.config_manager import ConfigManager
from src.common.enums.domain_source import DomainSource
from src.common.models.base_context import ContextBase
from injector import inject
from typing import Dict, List, Optional
import asyncio # Important: import asyncio for use in child classes

from src.common.models.ingestion_result import IngestionResult
from src.common.models.models import SummaryResultBase
from src.common.spark.spark_service import SparkService

class BaseDataReader(ABC):
    """
    An abstract base class for all data readers.
    It provides automatic data caching management.
    """
    @inject
    def __init__(self, spark: SparkService, context: ContextBase, config: ConfigManager):
        """
        Initializes the data reader with injected dependencies.

        Args:
            spark (SparkService): The Spark service instance.
            context (ContextBase): The context of the current process.
            config (ConfigManager): The configuration manager instance.
        """
        self._spark = spark
        self._context = context
        self._config = config
        self._loaded_dataframes: Dict[str, DataFrame] = {}

    def set_domain_source(self, domain_source: DomainSource):
        """
        Sets the domain source for the reader.

        Args:
            domain_source (DomainSource): The specific domain source to read data from.
        """
        self._domain_source = domain_source

    def load_data(self, dataset_names: Optional[List[str]] = None) -> Dict[str, DataFrame]:
        """
        The main method for reading data.
        It loads only the specified datasets or all available datasets if `dataset_names` is not set.
        It also handles caching.
        
        Args:
            dataset_names (Optional[List[str]]): A list of specific dataset names to load.
        
        Returns:
            Dict[str, DataFrame]: A dictionary of Spark DataFrames, where keys are dataset names.
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

        valid_ingestion_results = self._get_valid_ingestion_results()

        if not valid_ingestion_results:
            logger.warning(f"No ingestion results found for domain source '{self._domain_source.name}'.")
            return {}

        all_dataframes = self._load_from_source(valid_ingestion_results)
        
        if all_dataframes:
            self._context._cache.set(cache_key, all_dataframes)
            
            if dataset_names:
                return {name: df for name, df in all_dataframes.items() if name in dataset_names}
        
        return all_dataframes or {}
    
    def _get_valid_ingestion_results(self) -> List[SummaryResultBase]:
        """
        A private helper method to filter results from the context based on the domain source.
        
        Returns:
            List[SummaryResultBase]: A list of valid ingestion results.
        """
        if not self._context.summary or not self._context.summary.results:
            return []
        
        results = []
        for result in self._context.summary.results:
            if isinstance(result, SummaryResultBase) and result.domain_source == self._domain_source:
                results.append(result)

        return [r for r in results if r.is_valid()]

    @abstractmethod
    def _load_from_source(self, all_readers: Optional[List[SummaryResultBase]]) -> Dict[str, DataFrame]:
        """
        An abstract method that should contain the logic for actually
        loading data from a file and returning a dictionary of DataFrames.
        
        Args:
            all_readers (Optional[List[SummaryResultBase]]): The list of valid ingestion results
                                                             to guide the data loading process.
        
        Returns:
            Dict[str, DataFrame]: A dictionary of Spark DataFrames.
        """
        pass