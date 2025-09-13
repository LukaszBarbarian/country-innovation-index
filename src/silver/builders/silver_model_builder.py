# src/silver/builders/silver_model_builder.py
from typing import Dict, Optional, cast
from pyspark.sql import DataFrame
from injector import Injector, inject

from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.enums.reference_source import ReferenceSource
from src.common.readers.base_data_reader import BaseDataReader
from src.common.factories.data_reader_factory import DataReaderFactory
from src.common.config.config_manager import ConfigManager
from src.common.spark.spark_service import SparkService
from src.silver.context.silver_context import SilverContext
from src.silver.models.models import SilverManifestModel
from src.silver.models.process_model import SilverProcessModel
from src.silver.readers.manual_data_reader import ManualDataReader
from src.silver.readers.reference_data_reader import ReferenceDataReader


class SilverModelBuilder(BaseModelBuilder):
    """
    The base class for all Silver layer model builders.

    It provides core functionality for loading data from various sources (Bronze,
    manual, and reference data), handling a cache, and creating the final
    `SilverProcessModel` object.
    """
    @inject
    def __init__(
        self,
        spark_service: SparkService,
        injector: Injector,
        context: SilverContext,
        config: ConfigManager,
        manual_data_reader: ManualDataReader,
        reference_data_reader: ReferenceDataReader
    ):
        """
        Initializes the builder with dependency-injected services.
        """
        super().__init__(spark_service, injector, context, config)
        self._manual_data_reader = manual_data_reader
        self._reference_data_reader = reference_data_reader

    def load_data(self) -> Dict[DomainSource, Dict[str, DataFrame]]:
        """
        Loads the raw data required for building the model.

        It determines whether to load data from the Bronze layer or from
        manual data sources based on the manifest configuration and uses
        caching to avoid redundant reads.

        Returns:
            Dict[DomainSource, Dict[str, DataFrame]]: A dictionary of loaded DataFrames,
            keyed by domain source and dataset name.
        """
        context = cast(SilverContext, self._context)
        model_config = self._get_model_config()
        if not model_config:
            print(f"No configuration found for model '{self._model_type}'.")
            return {}

        raw_data: Dict[DomainSource, Dict[str, DataFrame]] = {}
        required_manual = {(m.domain_source, m.dataset_name) for m in context.manifest.manual_data_paths}

        for src_dataset in model_config.source_datasets:
            domain_source = src_dataset.domain_source
            dataset_name = src_dataset.dataset_name
            key = (domain_source, dataset_name)

            df: Optional[DataFrame] = None
            if key in required_manual:
                df = self._get_manual_data(domain_source, dataset_name)
            else:
                reader = self._get_bronze_reader(domain_source)
                loaded = reader.load_data([dataset_name])
                df = loaded.get(dataset_name)

            if df is not None:
                raw_data.setdefault(domain_source, {})[dataset_name] = df
            else:
                print(f"No data found for source {domain_source.value}, dataset {dataset_name}. Skipping.")

        return raw_data

    def _get_manual_data(self, domain_source: DomainSource, dataset_name: str) -> Optional[DataFrame]:
        """
        Loads and caches manual data.
        """
        context = cast(SilverContext, self._context)
        cache_key = f"manual:{domain_source.value}.{dataset_name}"
        if context._cache.exists(cache_key):
            return context._cache.get(cache_key)

        df = self._manual_data_reader.load_for_dataset(domain_source, dataset_name)
        if df is not None:
            context._cache.set(cache_key, df)
        return df

    def get_references(self, dataset_name: ReferenceSource) -> Optional[DataFrame]:
        """
        Loads and caches reference data.
        """
        context = cast(SilverContext, self._context)
        cache_key = f"references:{dataset_name}"
        if context._cache.exists(cache_key):
            return context._cache.get(cache_key)

        df = self._reference_data_reader.load_for_dataset(dataset_name)
        if df is not None:
            context._cache.set(cache_key, df)
        return df

    def _get_bronze_reader(self, domain_source: DomainSource) -> BaseDataReader:
        """
        Gets the appropriate data reader for a given Bronze domain source.
        """
        reader_class = DataReaderFactory.get_class(domain_source)
        reader = self._injector.get(reader_class)
        reader.set_domain_source(domain_source)
        return reader

    def _get_model_config(self) -> Optional[SilverManifestModel]:
        """
        Retrieves the model's configuration from the manifest.
        """
        if not self._model_type:
            raise ValueError("Model type not set. Call set_identity() first.")
        context = cast(SilverContext, self._context)
        for m in context.manifest.models:
            if m.model_name == self._model_type:
                return m
        return None

    def create_model(self, df: DataFrame) -> SilverProcessModel:
        """
        Creates a `SilverProcessModel` object with the processed DataFrame.
        """
        return SilverProcessModel(
            data=df,
            model_type=self._model_type
        )