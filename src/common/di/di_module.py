from injector import Module, singleton, Binder, provider
from src.common.contexts.layer_context import LayerContext

class DIModule(Module):
    def __init__(self, context: LayerContext):
        self._context = context