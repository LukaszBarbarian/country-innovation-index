# src/common/factories/data_reader_factory.py

from src.common.builders.analytical_builder import AnalyticalBaseBuilder
from src.common.enums.model_type import ModelType
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from typing import Type
from src.common.registers.analytical_model_registry import AnalyticalModelRegistry
from src.common.transformators.base_transformer import BaseTransformer


class AnalyticalModelFactory(BaseFactoryFromRegistry[str, AnalyticalBaseBuilder]):
    """
    A factory for creating instances of AnalyticalBaseBuilder based on a model type.
    """
    @classmethod
    def get_registry(cls) -> BaseRegistry[str, Type[AnalyticalBaseBuilder]]:
        """
        Returns the registry instance responsible for mapping model names to their builder classes.
        
        Returns:
            BaseRegistry[str, Type[AnalyticalBaseBuilder]]: An instance of the analytical model registry.
        """
        return AnalyticalModelRegistry()