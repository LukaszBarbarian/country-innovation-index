

from typing import Optional
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase


class BaseParser():
    def __init__(self, config: ConfigManager):
        self.config = config

    def parse(self, manifest_json: str, summary_json: Optional[str] = None) -> ContextBase:
        raise NotImplementedError("Must implement parse in subclass")