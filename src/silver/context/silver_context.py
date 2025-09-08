from dataclasses import dataclass
from typing import Optional

from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase
from src.silver.models.models import SilverManifest, SilverSummary


@dataclass
class SilverContext(ContextBase):
    summary: Optional[SilverSummary] = None
    manifest: Optional[SilverManifest] = None




import os
from dataclasses import dataclass, field
from typing import Optional, Dict
from src.common.enums.etl_layers import ETLLayer
from src.common.models.base_context import ContextBase
from src.silver.models.models import SilverManifest, SilverSummary


@dataclass
class SilverContext(ContextBase):
    summary: Optional[SilverSummary] = None
    manifest: Optional[SilverManifest] = None


    def __post_init__(self):
        """
        Metoda wywoływana po inicjalizacji obiektu,
        służąca do konwersji ścieżek na pełne URL.
        """
        self._convert_paths()
    
    def _convert_paths(self):
        """
        Konwertuje względne ścieżki w manifestach na pełne URL Azure Blob Storage.
        """
            
        # Konwersja ścieżek w references_tables
        if self.manifest.references_tables:
            self.manifest.references_tables = {
                key: self._build_full_url(value, super().storage_account)
                for key, value in self.manifest.references_tables.items()
            }

        # Konwersja ścieżek w manual_data_paths
        if self.manifest.manual_data_paths:
            for item in self.manifest.manual_data_paths:
                item.file_path = self._build_full_url(item.file_path, super().storage_account)

    def _build_full_url(self, relative_path: str, storage_account: str) -> str:
        """
        Tworzy pełny URL Azure Blob Storage z relatywnej ścieżki.
        """
        return f"https://{storage_account}.blob.core.windows.net{relative_path}"    