from dataclasses import dataclass
import datetime
from typing import Optional

from src.bronze.models.manifest import BronzeManifest
from src.common.models.base_context import ContextBase


@dataclass
class BronzeContext(ContextBase):
    manifest: Optional[BronzeManifest] = None
    ingestion_date = datetime.datetime.utcnow()

