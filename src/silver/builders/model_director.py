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
    """
    The ModelDirector is a core component of the Silver ETL layer, responsible for
    orchestrating the end-to-end process of building a single analytical model.

    This class handles:
    - Dependency management: It recursively calls itself to build any prerequisite models.
    - Caching: It stores and retrieves built models to prevent redundant computation.
    - Data loading and transformation: It fetches raw data and applies the correct
      domain-specific transformations (transform, normalize, enrich) using a factory.
    - Model building: It uses the appropriate ModelBuilder to construct the final DataFrame.
    """
    @inject
    def __init__(self, injector: Injector, context: SilverContext):
        """
        Initializes the ModelDirector with a dependency injector and the Silver context.
        """
        self._context = context
        self._injector = injector

    async def get_built_model(self, model: SilverManifestModel) -> SilverProcessModel:
        """
        Builds a Silver model based on its manifest configuration.
        This method handles dependencies recursively and uses a cache.

        Args:
            model (SilverManifestModel): The configuration object for the model to build.

        Returns:
            SilverProcessModel: A dataclass containing the built DataFrame and metadata.
        
        Raises:
            RuntimeError: If model configuration or raw data is missing.
        """
        cache_key = f"silver_model:{model.model_name.value}"

        # 🔹 Cache check
        if self._context._cache.exists(cache_key):
            print(f"[CACHE HIT] Retrieved model '{model.model_name.value}' from cache.")
            return self._context._cache.get(cache_key)

        # 🔹 Get the builder
        builder_class = ModelBuilderFactory.get_class(model.model_name)
        builder = self._injector.get(builder_class)
        builder.set_identity(model.model_name)

        # 🔹 Get model configuration from manifest
        model_config = next(
            (m for m in self._context.manifest.models if m.model_name == model.model_name),
            None,
        )
        if model_config is None:
            raise RuntimeError(
                f"Missing configuration for model '{model.model_name.value}' in SilverManifest."
            )

        # 🔹 Handle dependencies
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
                dep_tasks.append(self.get_built_model(dep_manifest))  # recursion

            dep_results: List[SilverProcessModel] = await asyncio.gather(*dep_tasks)
            for dep_res in dep_results:
                dependencies[dep_res.model_type] = dep_res.data

        # 🔹 Get raw data
        raw_dataframes: Dict[DomainSource, Dict[str, DataFrame]] = builder.load_data()
        if not raw_dataframes:
            if builder.synthetic:
                raw_dataframes = {}
            else:
                raise RuntimeError(f"No raw data to build model '{model.model_name.value}'.")

        # 🔹 Process data with transformers
        processed_dfs: Dict[Tuple[DomainSource, str], DataFrame] = await self._process_dataframes(raw_dataframes)

        # 🔹 Build the final model
        final_df = await builder.build(processed_dfs, dependencies)
        built_model = builder.create_model(final_df)

        # 🔹 Cache the model
        built_model.data.cache()
        self._context._cache.set(cache_key, built_model)
        print(f"[BUILD] Model '{model.model_name.value}' has been built and saved to cache.")

        return built_model

    async def _process_dataframes(
        self,
        raw_dataframes: Dict[DomainSource, Dict[str, DataFrame]]
    ) -> Dict[Tuple[DomainSource, str], DataFrame]:
        """
        Processes all raw DataFrames through the appropriate transformers:
        - transform
        - normalize
        - enrich
        
        Args:
            raw_dataframes (Dict): A dictionary of raw DataFrames grouped by domain source.

        Returns:
            Dict: A dictionary of processed DataFrames, with a composite key of (domain_source, dataset_name).
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