import os
from dataclasses import dataclass, field
from typing import Optional, Dict
from src.common.enums.etl_layers import ETLLayer
from src.common.models.base_context import ContextBase
from src.silver.models.models import SilverManifest, SilverSummary


@dataclass
class SilverContext(ContextBase):
    """
    A dataclass representing the context for a Silver ETL run.

    This class holds all necessary runtime information, including the manifest,
    summary, and base context properties inherited from `ContextBase`. It
    automatically converts relative file paths in the manifest to full Azure
    Blob Storage URLs during initialization.
    """
    summary: Optional[SilverSummary] = None
    manifest: Optional[SilverManifest] = None
    
    def __post_init__(self):
        """
        Method called after object initialization, used to convert paths to full URLs.
        """
        self._convert_paths()
    
    def _convert_paths(self):
        """
        Converts relative paths in manifests to full Azure Blob Storage URLs.
        """
        # Convert paths in references_tables
        if self.manifest and self.manifest.references_tables:
            self.manifest.references_tables = {
                key: self._build_full_url(value, self.storage_account)
                for key, value in self.manifest.references_tables.items()
            }

        # Convert paths in manual_data_paths
        if self.manifest and self.manifest.manual_data_paths:
            for item in self.manifest.manual_data_paths:
                item.file_path = self._build_full_url(item.file_path, self.storage_account)

    def _build_full_url(self, relative_path: str, storage_account: str) -> str:
        """
        Creates a full Azure Blob Storage URL from a relative path.
        """
        # Ensure the path starts with a '/', add if missing.
        if not relative_path.startswith('/'):
            relative_path = '/' + relative_path

        return f"https://{storage_account}.blob.core.windows.net{relative_path}"