# src/functions/common/processor_registry.py

from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.processors.base_data_processor import BaseDataProcessor
from typing import Dict, Type


class DataProcessorRegistry(BaseRegistry[DomainSource, BaseDataProcessor]):
    """
    A registry for mapping domain sources to their corresponding data processor classes.
    This class inherits from `BaseRegistry` to provide a centralized and consistent
    way to manage and retrieve registered data processors.
    """
    _registry: Dict[DomainSource, Type[BaseDataProcessor]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[BaseDataProcessor]]:
        """
        Returns the internal dictionary that stores the registered data processor classes.
        This method is required by the base class to access the registry data.
        """
        return cls._registry