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
    A request object for a Builder, containing the model specification and
    the results from upstream processes (e.g., from the Silver layer).
    """
    model: GoldManifestModel
    source_results: List[ProcessModelResult]  # A list of ProcessModelResult objects.
    loaded_dfs: Optional[Dict[ModelType, DataFrame]] = None

    def get_df(self, model_type: ModelType) -> Optional[DataFrame]:
        """
        Retrieves the DataFrame for a specific model type if it has been loaded.

        Args:
            model_type (ModelType): The type of the model to retrieve.

        Returns:
            Optional[DataFrame]: The Spark DataFrame if it exists, otherwise None.
        """
        if not self.loaded_dfs:
            return None
        return self.loaded_dfs.get(model_type)