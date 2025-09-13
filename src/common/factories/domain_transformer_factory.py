# src/common/factories/data_reader_factory.py

from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.readers.base_data_reader import BaseDataReader
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.registers.data_reader_registry import DataReaderRegistry
from typing import Type

from src.common.registers.domain_transformer_registry import DomainTransformerRegistry
from src.common.transformators.base_transformer import BaseTransformer


class DomainTransformerFactory(BaseFactoryFromRegistry[DomainSource, BaseTransformer]):
    """
    A factory for creating instances of domain transformers based on a DomainSource.
    """
    @classmethod
    def get_registry(cls) -> BaseRegistry[DomainSource, Type[BaseTransformer]]:
        """
        Returns the DomainTransformerRegistry instance.

        This method provides the specific registry required by the BaseFactoryFromRegistry
        to look up and instantiate the correct transformer class.
        """
        return DomainTransformerRegistry()