from dataclasses import dataclass, field
from typing import Dict, List

from src.common.enums.etl_layers import ETLLayer
from src.common.models.manifest import ManifestBase, ManualDataPath, Model


@dataclass(frozen=True)
class SilverManifest(ManifestBase):
    """Główna klasa manifestu dla warstwy Silver."""
    references_tables: Dict[str, str] = field(default_factory=dict)
    manual_data_paths: List[ManualDataPath] = field(default_factory=list)
    models: List[Model] = field(default_factory=list)