# src/silver/factories/model_builder_factory.py
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.model_type import ModelType
from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.factories.base_factory import BaseFactoryFromRegistry


class ModelBuilderFactory(BaseFactoryFromRegistry[ModelType, BaseModelBuilder]):
    """
    A factory responsible for creating instances of BaseModelBuilders.
    It uses the ModelBuilderRegistry to retrieve the correct classes for instantiation.
    """
    @classmethod
    def get_registry(cls) -> BaseRegistry[ModelType, BaseModelBuilder]:
        """
        Returns the instance of the ModelBuilderRegistry, which stores the
        registered ModelBuilder classes.
        
        This method is a required part of the factory pattern, connecting
        the factory to its specific registry.
        """
        return ModelBuilderRegistry()