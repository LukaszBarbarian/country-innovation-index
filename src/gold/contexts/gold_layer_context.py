from dataclasses import dataclass
from typing import Optional
from src.common.models.base_context import ContextBase
from src.gold.models.models import GoldManifest, GoldSummary


@dataclass
class GoldContext(ContextBase):
    """
    A dataclass representing the context for a Gold layer ETL process.
    It inherits from `ContextBase` and adds specific fields for the Gold layer,
    including the `GoldSummary` and `GoldManifest`.
    """
    summary: Optional[GoldSummary] = None
    manifest: Optional[GoldManifest] = None