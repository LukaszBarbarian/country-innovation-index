# src/silver/mediators/model_builder_mediator.py
from injector import Injector
from src.silver.contexts.layer_runtime_context import LayerRuntimeContext
from src.common.models.mediator_result import MediatorResult
from src.common.factories.model_builder_factory import ModelBuilderFactory
from src.common.enums.model_type import ModelType
from src.common.builders.base_model_builder import BaseModelBuilder

class ModelBuilderMediator:
    def __init__(self, di_injector: Injector, context: LayerRuntimeContext):
        self.context = context
        self.injector = di_injector

    async def run(self) -> MediatorResult:
        print("ModelBuilderMediator: Rozpoczynam proces budowania...")

        return MediatorResult()
    


    def create_model(self, model_type: ModelType):
            """Pobiera buildera z 'injector' i tworzy model."""
            builder_class = ModelBuilderFactory.get_class(model_type)
            builder_instance: BaseModelBuilder = self.injector.get(builder_class)
            return builder_instance.run()