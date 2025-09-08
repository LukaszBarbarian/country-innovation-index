# src/common/registers/data_reader_registry.py
from typing import Dict, Type
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.transformators.base_transformer import BaseTransformer

class DomainTransformerRegistry(BaseRegistry[DomainSource, BaseTransformer]):
    _registry: Dict[DomainSource, Type[BaseTransformer]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[BaseTransformer]]:
        return cls._registry
        