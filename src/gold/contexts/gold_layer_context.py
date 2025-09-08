from dataclasses import dataclass
from typing import Optional
from src.common.models.base_context import ContextBase
from src.gold.models.models import GoldManifest, GoldSummary


@dataclass
class GoldContext(ContextBase):
    summary: Optional[GoldSummary] = None
    manifest: Optional[GoldManifest] = None