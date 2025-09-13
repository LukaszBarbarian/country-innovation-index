from typing import Dict, Type
from src.common.builders.analytical_builder import AnalyticalBaseBuilder
from src.common.enums.model_type import ModelType
from src.common.registers.base_registry import BaseRegistry


class AnalyticalModelRegistry(BaseRegistry[str, AnalyticalBaseBuilder]):
    """
    A registry for mapping analytical model names to their corresponding builder classes.
    This class inherits from `BaseRegistry` to provide a consistent way of managing
    and retrieving registered builder classes.
    """
    _registry: Dict[str, Type[AnalyticalBaseBuilder]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[str, Type[AnalyticalBaseBuilder]]:
        """
        Returns the internal dictionary that stores the registered builder classes.
        This method is required by the base class to access the registry data.
        """
        return cls._registry