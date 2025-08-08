from injector import Module, singleton, Binder, provider
from src.common.config.config_manager import ConfigManager
from src.common.contexts.base_layer_context import BaseLayerContext

class DIModule(Module):
    def __init__(self, context: BaseLayerContext, config: ConfigManager):
        self._context = context
        self._config = config