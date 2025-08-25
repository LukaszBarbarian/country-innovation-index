# src/silver/model_builders/silver_model_builder.py
from abc import abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, List, Optional, cast
from injector import Injector, inject

from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.enums.domain_source import DomainSource
from src.common.models.manifest import Model
from src.common.readers.base_data_reader import BaseDataReader
from src.common.factories.data_reader_factory import DataReaderFactory

from src.silver.builders.model_director import ModelDirector
from src.silver.context.silver_context import SilverLayerContext
from src.silver.readers.manual_data_reader import ManualDataReader
from src.silver.readers.reference_data_reader import ReferenceDataReader
from src.common.spark.spark_service import SparkService
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import BaseContext


class SilverModelBuilder(BaseModelBuilder):
    @inject
    def __init__(self, 
                 spark: SparkService, 
                 injector: Injector, 
                 context: BaseContext, 
                 config: ConfigManager, 
                 manual_data_reader: ManualDataReader, 
                 reference_data_reader: ReferenceDataReader):
        
        super().__init__(spark, injector, context, config)
        self._manual_data_reader = manual_data_reader
        self._reference_data_reader = reference_data_reader
        
    def load_data(self) -> Dict[DomainSource, Dict[str, DataFrame]]:
        self._context = cast(SilverLayerContext, self._context)
        model_config = self._get_model_config()
        
        if not model_config:
            print(f"Brak konfiguracji dla modelu '{self._model_type}'.")
            return {}

        raw_data: Dict[DomainSource, Dict[str, DataFrame]] = {}
        
        # Tworzymy zestaw tupli dla szybkiego wyszukiwania wymaganych danych manualnych
        required_manual_data = {
            (path.domain_source, path.dataset_name)
            for path in self._context.manual_data_paths
        }

        # Iterujemy tylko raz przez wymagane źródła danych
        for source_dataset in model_config.source_datasets:
            domain_source = source_dataset.domain_source
            dataset_name = source_dataset.dataset_name
            source_tuple = (domain_source, dataset_name)
            
            df: Optional[DataFrame] = None
            
            # Sprawdzamy, czy dane manualne dla tego źródła są dostępne
            if source_tuple in required_manual_data:
                df = self.get_manual(domain_source, dataset_name)
            else:
                # W przeciwnym razie, załaduj z warstwy Bronze
                reader = self.get_bronze_reader(domain_source)
                loaded_data = reader.load_data([dataset_name])
                df = loaded_data.get(dataset_name)
            
            if df:
                if domain_source not in raw_data:
                    raw_data[domain_source] = {}
                raw_data[domain_source][dataset_name] = df
            else:
                print(f"Brak danych dla źródła: '{domain_source.value}', dataset: '{dataset_name}'. Pomijam.")
        
        return raw_data
    
    def get_manual(self, domain_source: DomainSource, dataset: str) -> Optional[DataFrame]:
        cache_key = f"manual:{domain_source.value}.{dataset}"
        if self._context._cache.exists(cache_key):
            return self._context._cache.get(cache_key)
        
        df = self._manual_data_reader.load_for_dataset(domain_source, dataset)
        if df is not None:
            self._context._cache.set(cache_key, df)
        return df

    def get_references(self, dataset_name: str) -> Optional[DataFrame]:
        cache_key = f"references:{dataset_name}"
        if self._context._cache.exists(cache_key):
            return self._context._cache.get(cache_key)

        df = self._reference_data_reader.load_for_dataset(dataset_name)
        if df is not None:
            self._context._cache.set(cache_key, df)
        return df

    def get_bronze_reader(self, domain_source: DomainSource) -> BaseDataReader:
        reader_class = DataReaderFactory.get_class(domain_source)
        reader = self._injector.get(reader_class)
        reader.set_domain_source(domain_source)
        return reader
        
    def _get_model_config(self) -> Model:
        if not self._model_type:
            raise ValueError("Model type not set. Call set_identity() first.")
        
        for model in self._context.models:
            if model.model_name == self._model_type:
                return model
        return None