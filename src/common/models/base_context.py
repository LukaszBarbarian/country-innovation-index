# src/common/contexts/base_layer_context.py

from abc import abstractmethod
from dataclasses import dataclass, field
import datetime
import os
from typing import Dict, Any, List, Optional
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.models.manifest import ManifestBase
from src.common.models.models import SummaryBase
from src.common.utils.cache import Cache

@dataclass
class ContextBase:
    env: Env
    etl_layer: ETLLayer
    storage_account: str = ""
    summary: Optional[SummaryBase] = None
    manifest: Optional[ManifestBase] = None
    correlation_id: str = ""

    _cache: Cache = field(default_factory=Cache, repr=False, init=False)



    # Funkcja pomocnicza do generowania ścieżki
    def storage_path_abfss(self, relative_path: str) -> str:
        return f"abfss://{self.storage_account}@{self.storage_account}.dfs.core.windows.net/{relative_path}"

    def __post_init__(self):
        self.storage_account = ConfigManager().get("DATA_LAKE_STORAGE_ACCOUNT_NAME")