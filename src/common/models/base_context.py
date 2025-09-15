# src/common/contexts/base_layer_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.models.manifest import ManifestBase
from src.common.models.models import SummaryBase
from src.common.utils.cache import Cache

@dataclass
class ContextBase:
    """
    A dataclass representing the base context for an ETL process.
    It encapsulates key environmental and process-specific information,
    such as the environment, ETL layer, and process IDs. It also includes
    a utility method for generating storage paths.
    """
    env: Env
    etl_layer: ETLLayer
    storage_account: str = ""
    summary: Optional[SummaryBase] = None
    manifest: Optional[ManifestBase] = None
    correlation_id: str = ""

    _cache: Cache = field(default_factory=Cache, repr=False, init=False)

    def storage_path_abfss(self, relative_path: str) -> str:
        """
        Generates a full ABFSS (Azure Blob File System) path.

        This utility method constructs a complete URL for a given relative path
        within the Azure Data Lake Storage account.

        Args:
            relative_path (str): The relative path within the storage account.

        Returns:
            str: The full ABFSS URL.
        """
        return f"abfss://{self.storage_account}@{self.storage_account}.dfs.core.windows.net/{relative_path}"