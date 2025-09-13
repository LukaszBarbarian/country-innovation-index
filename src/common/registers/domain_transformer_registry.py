# src/common/registers/data_reader_registry.py
from typing import Dict, Type
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.transformators.base_transformer import BaseTransformer

class DomainTransformerRegistry(BaseRegistry[DomainSource, BaseTransformer]):
    """
    A registry for mapping domain sources to their corresponding transformer classes.
    This class inherits from `BaseRegistry` to provide a consistent way to manage
    and retrieve registered domain transformers.
    """
    _registry: Dict[DomainSource, Type[BaseTransformer]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[BaseTransformer]]:
        """
        Returns the internal dictionary that stores the registered transformer classes.
        This method is required by the base class to access the registry data.
        """
        return cls._registry