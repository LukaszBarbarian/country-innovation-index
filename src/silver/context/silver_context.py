# src/common/contexts/silver_layer_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional

# Importujemy nowy model wyników
from src.common.models.base_context import BaseContext
from src.common.models.ingestions import IngestionSummary
from src.common.models.manifest import Model


@dataclass(frozen=True, kw_only=True)
class SilverLayerContext(BaseContext):
    references_tables: Dict[str, str]
    models: List[Model]
    ingestion_summary: IngestionSummary