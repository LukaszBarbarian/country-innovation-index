from dataclasses import dataclass
from typing import List
from src.common.models.manifest import ManifestBase, SourceConfigPayload


@dataclass(frozen=True)
class BronzeManifest(ManifestBase):
    """Klasa dla całego manifestu warstwy Bronze."""
    sources: List[SourceConfigPayload]