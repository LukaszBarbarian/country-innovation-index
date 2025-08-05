from injector import Module, singleton, Binder, provider
from src.common.contexts.base_layer_context import BaseLayerContext

class DIModule(Module):
    def __init__(self, context: BaseLayerContext):
        self._context = context