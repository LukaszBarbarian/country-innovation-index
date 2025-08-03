import injector
from src.common.enums.model_type import ModelType
from src.common.factories.model_builder_factory import ModelBuilderFactory


class ModelModule(injector.Module):
    def configure(self, binder):

        for model_type in ModelType:
            builder_class = ModelBuilderFactory.get_class(model_type)

            binder.bind(builder_class, to=builder_class, scope=injector.singleton)
