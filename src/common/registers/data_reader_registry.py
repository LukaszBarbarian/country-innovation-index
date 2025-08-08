# src/common/registers/data_reader_registry.py
from typing import Dict, Type
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.readers.base_data_reader import BaseDataReader

class DataReaderRegistry(BaseRegistry[DomainSource, BaseDataReader]):
    _registry: Dict[DomainSource, Type[BaseDataReader]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[BaseDataReader]]:
        return cls._registry