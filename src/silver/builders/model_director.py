import asyncio
from typing import Dict, Tuple, List
from pyspark.sql import DataFrame
from injector import inject, Injector, singleton

from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.common.factories.model_builder_factory import ModelBuilderFactory
from src.common.factories.domain_transformer_factory import DomainTransformerFactory
from src.silver.context.silver_context import SilverContext
from src.silver.models.models import SilverManifestModel
from src.silver.models.process_model import SilverProcessModel


@singleton
class ModelDirector:
    @inject
    def __init__(self, injector: Injector, context: SilverContext):
        self._context = context
        self._injector = injector

    async def get_built_model(self, model: SilverManifestModel) -> SilverProcessModel:
        """
        Buduje model silver na podstawie manifestu. 
        Obsługuje zależności (rekurencyjnie) i cache.
        """
        cache_key = f"silver_model:{model.model_name.value}"

        # 🔹 Sprawdzenie cache
        if self._context._cache.exists(cache_key):
            print(f"[CACHE HIT] Pobrano model '{model.model_name.value}' z cache.")
            return self._context._cache.get(cache_key)

        # 🔹 Pobranie buildera
        builder_class = ModelBuilderFactory.get_class(model.model_name)
        builder = self._injector.get(builder_class)
        builder.set_identity(model.model_name)

        # 🔹 Pobranie konfiguracji modelu z manifestu
        model_config = next(
            (m for m in self._context.manifest.models if m.model_name == model.model_name),
            None,
        )
        if model_config is None:
            raise RuntimeError(
                f"Brak konfiguracji dla modelu '{model.model_name.value}' w SilverManifest."
            )

        # 🔹 Obsługa zależności
        dependencies: Dict[ModelType, DataFrame] = {}
        depends_on: List[ModelType] = getattr(model_config, "depends_on", []) or []

        if depends_on:
            dep_tasks = []
            for dep_type in depends_on:
                dep_manifest = next(
                    (m for m in self._context.manifest.models if m.model_name == dep_type),
                    None,
                )
                if dep_manifest is None:
                    raise RuntimeError(
                        f"Dependency '{dep_type}' not found in manifest for model '{model.model_name.value}'"
                    )
                dep_tasks.append(self.get_built_model(dep_manifest))  # rekurencja

            dep_results: List[SilverProcessModel] = await asyncio.gather(*dep_tasks)
            for dep_res in dep_results:
                dependencies[dep_res.model_type] = dep_res.data

        # 🔹 Pobranie surowych danych
        raw_dataframes: Dict[DomainSource, Dict[str, DataFrame]] = builder.load_data()
        if not raw_dataframes:
            if builder.synthetic:
                raw_dataframes = {}
            else:
                raise RuntimeError(f"Brak surowych danych do budowy modelu '{model.model_name.value}'.")

        # 🔹 Przetworzenie danych przez transformatory
        processed_dfs: Dict[Tuple[DomainSource, str], DataFrame] = await self._process_dataframes(raw_dataframes)

        # 🔹 Budowa modelu końcowego
        final_df = await builder.build(processed_dfs, dependencies)
        built_model = builder.create_model(final_df)

        # 🔹 Cache'owanie modelu
        built_model.data.cache()
        self._context._cache.set(cache_key, built_model)
        print(f"[BUILD] Model '{model.model_name.value}' został zbudowany i zapisany w cache.")

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
        """
        processed_dfs: Dict[Tuple[DomainSource, str], DataFrame] = {}

        for domain_source, datasets in raw_dataframes.items():
            transformer = DomainTransformerFactory.get_instance(domain_source)

            for dataset_name, df in datasets.items():
                transformed_df = await transformer.transform(df, dataset_name)
                normalized_df = await transformer.normalize(transformed_df, dataset_name)
                enriched_df = await transformer.enrich(normalized_df, dataset_name)

                processed_dfs[(domain_source, dataset_name)] = enriched_df

        return processed_dfs
