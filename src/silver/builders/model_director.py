# src/silver/orchestrators/model_director.py

from typing import Dict, Tuple
from pyspark.sql import DataFrame
from injector import inject, Injector, singleton

from src.common.enums.model_type import ModelType
from src.common.factories.model_builder_factory import ModelBuilderFactory
from src.common.models.etl_model import EtlModel
from src.common.enums.domain_source import DomainSource
from src.common.factories.domain_transformer_factory import DomainTransformerFactory
from src.silver.context.silver_context import SilverLayerContext


@singleton
class ModelDirector:
    @inject
    def __init__(self, injector: Injector, context: SilverLayerContext):
        self._context = context
        self._injector = injector

    async def get_built_model(self, model_type: ModelType) -> EtlModel:
        cache_key = f"silver_model:{model_type.value}"

        if self._context._cache.exists(cache_key):
            print(f"Pobrano model '{model_type.value}' z cache.")
            return self._context._cache.get(cache_key)

        builder_class = ModelBuilderFactory.get_class(model_type)
        builder = self._injector.get(builder_class)
        builder.set_identity(model_type)

        # Pobranie konfiguracji modelu
        model_config = next(m for m in self._context.models if m.model_name == model_type)

        # Budowa zależności
        dependencies = {}
        for dep_model_type in getattr(model_config, "depends_on", []) or []:
            dep_model = await self.get_built_model(dep_model_type)
            dependencies[dep_model_type] = dep_model.data

        # Pobranie surowych danych
        raw_dataframes: Dict[DomainSource, Dict[str, DataFrame]] = builder.load_data()
        if not raw_dataframes:
            raise RuntimeError(f"Brak surowych danych do budowy modelu '{model_type.value}'.")

        # Przetworzenie surowych danych przez transformery
        processed_dfs: Dict[Tuple[DomainSource, str], DataFrame] = await self._process_dataframes(raw_dataframes)

        # Budowa modelu z przetworzonych danych
        final_df = await builder.build(processed_dfs, dependencies)
        built_model = builder.create_model(final_df)

        # Cache'owanie
        built_model.data.cache()
        self._context._cache.set(cache_key, built_model)
        print(f"Model '{model_type.value}' został zbudowany i zcache'owany.")

        return built_model

    async def _process_dataframes(
        self, 
        raw_dataframes: Dict[DomainSource, Dict[str, DataFrame]]
    ) -> Dict[Tuple[DomainSource, str], DataFrame]:
        """
        Przetwarza wszystkie surowe DataFrame'y przez odpowiednie transformatory:
        - transform
        - normalize
        - enrich
        Zwraca słownik z kluczem (DomainSource, dataset_name)
        """
        processed_dfs: Dict[Tuple[DomainSource, str], DataFrame] = {}

        for domain_source, datasets in raw_dataframes.items():
            transformer = DomainTransformerFactory.get_instance(domain_source)

            for dataset_name, df in datasets.items():
                transformed_df = await transformer.transform(df, dataset_name)
                normalized_df = await transformer.normalize(transformed_df, dataset_name)
                enriched_df = await transformer.enrich(normalized_df, dataset_name)

                # Klucz jako krotka (źródło, nazwa datasetu)
                processed_dfs[(domain_source, dataset_name)] = enriched_df

        return processed_dfs
