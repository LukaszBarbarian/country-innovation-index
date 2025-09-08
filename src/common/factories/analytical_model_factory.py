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
    Fabryka do tworzenia instancji DataReaderów na podstawie ModelType.
    """
    @classmethod
    def get_registry(cls) -> BaseRegistry[str, Type[AnalyticalBaseBuilder]]:
        """
        Zwraca instancję DataReaderRegistry.
        """
        return AnalyticalModelRegistry()