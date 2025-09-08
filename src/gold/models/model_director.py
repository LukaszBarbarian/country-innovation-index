# src/gold/models/model_director.py
import asyncio
import logging
from typing import List, Dict, Optional, cast
from injector import inject, Injector
from pyspark.sql import DataFrame

from src.common.enums.model_type import ModelType
from src.common.factories.analytical_model_factory import AnalyticalModelFactory
from src.common.models.build_request import BuildRequest
from src.common.models.models import SummaryResultBase # Correct import for previous layer's results
from src.common.spark.spark_service import SparkService
from src.gold.contexts.gold_layer_context import GoldContext
from src.gold.models.models import GoldManifestModel
from src.gold.models.process_model import GoldProcessModel
from src.gold.orchestrator.model_loader import ModelLoader

logger = logging.getLogger(__name__)

class ModelDirector:
    @inject
    def __init__(self, injector: Injector, context: GoldContext, spark: SparkService, model_loader: ModelLoader):
        self._injector = injector
        self._context = context
        self._spark = spark
        self._model_loader = model_loader

    async def get_built_model(self, model: GoldManifestModel) -> GoldProcessModel:
        """
        Buduje model warstwy GOLD (DIM lub FACT) na bazie wpisu z manifestu (GoldManifestModel).
        1) dopasowuje wymagane wyniki z poprzedniej warstwy (summary.results),
        2) ładuje DataFrame'y z output_path,
        3) uruchamia dedykowany builder (po nazwie modelu),
        4) zwraca GoldProcessModel z gotowym DataFrame'em.
        """

        # 1) Mapuj wyniki z poprzedniej warstwy po ModelType
        summary_results: List[SummaryResultBase] = self._context.summary.results or []
        results_map: Dict[ModelType, SummaryResultBase] = {
            r.model: r for r in summary_results if r.is_valid()
        }

        # 2) Zbierz wymagane źródła i waliduj brakujące
        missing = [m for m in model.source_models if m not in results_map]
        if missing:
            raise ValueError(
                f"[{model.name}] Brakuje wyników z poprzedniej warstwy dla modeli: {missing}"
            )

        relevant_results: List[SummaryResultBase] = [results_map[m] for m in model.source_models]

        # 3) Załaduj DataFrame'y asynchronicznie (z output_path)
        load_tasks = [asyncio.to_thread(self._model_loader.load, r.output_path) for r in relevant_results]
        loaded_dfs_list: List[DataFrame] = await asyncio.gather(*load_tasks)

        loaded_dfs: Dict[ModelType, DataFrame] = {
            model_type: loaded_dfs_list[idx]
            for idx, model_type in enumerate(model.source_models)
        }

        # 4) Zbuduj BuildRequest i odpal builder
        #    Uwaga: przekazujemy cały obiekt manifestu jako spec (builder może potrzebować primary_keys, type, itp.)
        request = BuildRequest(
            model=model,
            source_results=relevant_results,
            loaded_dfs=loaded_dfs
        )

        builder_class = AnalyticalModelFactory.get_class(model.name)
        builder = self._injector.get(builder_class)

        logger.info(f"[{model.name}] Uruchamiam builder: {builder_class.__name__}")
        df: DataFrame = await builder.run(request)

        # 5) Zwróć GoldProcessModel
        return GoldProcessModel(
            name=model.name,
            type=model.type,
            data=df
        )