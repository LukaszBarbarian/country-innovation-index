from src.common.registers.base_registry import BaseRegistry
from src.common.enums.model_type import ModelType
from src.common.builders.base_model_builder import BaseModelBuilder
from typing import Dict, Type

class ModelBuilderRegistry(BaseRegistry[ModelType, BaseModelBuilder]):
    """
    A registry for mapping model types to their corresponding builder classes.
    This class inherits from `BaseRegistry` to provide a consistent way of
    managing and retrieving registered model builders.
    """
    _registry: Dict[ModelType, Type[BaseModelBuilder]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[ModelType, Type[BaseModelBuilder]]:
        """
        Returns the internal dictionary that stores the registered model builder classes.
        This method is required by the base class to access the registry data.
        """
        return cls._registry