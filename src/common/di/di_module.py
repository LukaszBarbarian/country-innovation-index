from injector import Module, singleton, Binder, provider
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import BaseContext

class DIModule(Module):
    def __init__(self, context: BaseContext, config: ConfigManager):
        self._context = context
        self._config = config