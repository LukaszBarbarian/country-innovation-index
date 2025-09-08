from typing import Dict, Type
from src.common.builders.analytical_builder import AnalyticalBaseBuilder
from src.common.enums.model_type import ModelType
from src.common.registers.base_registry import BaseRegistry


class AnalyticalModelRegistry(BaseRegistry[str, AnalyticalBaseBuilder]):
    _registry: Dict[str, Type[AnalyticalBaseBuilder]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[str, Type[AnalyticalBaseBuilder]]:
        return cls._registry