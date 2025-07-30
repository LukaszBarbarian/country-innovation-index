# src/functions/common/processor_registry.py

from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.processors.base_data_processor import BaseDataProcessor
from typing import Dict, Type


class DataProcessorRegistry(BaseRegistry[DomainSource, BaseDataProcessor]):
    _registry: Dict[DomainSource, Type[BaseDataProcessor]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[BaseDataProcessor]]:
        return cls._registry