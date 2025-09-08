

from typing import Optional
from src.common.models.base_context import ContextBase


class BaseParser():
    def __init__(self):
        pass

    def parse(self, manifest_json: str, summary_json: Optional[str] = None) -> ContextBase:
        raise NotImplementedError("Must implement parse in subclass")