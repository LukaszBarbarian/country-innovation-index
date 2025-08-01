# Używamy BaseRegistry, którą już masz
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.model_type import ModelType
from src.common.builders.base_model_builder import BaseModelBuilder
from typing import Dict, Type

class ModelBuilderRegistry(BaseRegistry[ModelType, BaseModelBuilder]):
    _registry: Dict[ModelType, Type[BaseModelBuilder]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[ModelType, Type[BaseModelBuilder]]:
        return cls._registry