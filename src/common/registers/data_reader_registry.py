# src/common/registers/data_reader_registry.py
from typing import Dict, Type
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.readers.base_data_reader import BaseDataReader

class DataReaderRegistry(BaseRegistry[DomainSource, BaseDataReader]):
    """
    A registry for mapping domain sources to their corresponding data reader classes.
    This class inherits from `BaseRegistry` to provide a centralized and consistent
    way to manage and retrieve registered data readers.
    """
    _registry: Dict[DomainSource, Type[BaseDataReader]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[BaseDataReader]]:
        """
        Returns the internal dictionary that stores the registered data reader classes.
        This method is required by the base class to access the registry data.
        """
        return cls._registry