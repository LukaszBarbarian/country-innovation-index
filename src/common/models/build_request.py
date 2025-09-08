from pyspark.sql import DataFrame
from dataclasses import dataclass
from typing import Dict, List, Optional
from src.common.enums.model_type import ModelType
from src.common.models.model_spec import ModelSpec
from src.common.models.process_model_result import ProcessModelResult
from src.gold.models.models import GoldManifestModel


@dataclass
class BuildRequest:
    """
    Żądanie budowy dla Buildera: spec + konkretne ModelRunResulty (wejścia Silver).
    loaded_dfs: po załadowaniu przez Director - mapping ModelType -> DataFrame.
    """
    model: GoldManifestModel
    source_results: List[ProcessModelResult]  # lista ModelRunResult (typ z Twojego projektu)
    loaded_dfs: Optional[Dict[ModelType, DataFrame]] = None

    def get_df(self, model_type: ModelType):
        """Zwraca DataFrame dla model_type (jeśli załadowany)."""
        if not self.loaded_dfs:
            return None
        return self.loaded_dfs.get(model_type)    