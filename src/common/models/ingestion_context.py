from dataclasses import dataclass
from src.common.models.base_context import BaseContext
from src.common.models.manifest import SourceConfigPayload


@dataclass(frozen=True, kw_only=True)
class IngestionContext(BaseContext):
    source_config: SourceConfigPayload