# src/silver/model_builders/base_model_builder.py (bez zmian w tej klasie względem ostatniej propozycji)

from abc import ABC, abstractmethod
from src.silver.contexts.layer_runtime_context import LayerRuntimeContext
from src.silver.contexts.silver_context import SilverContext
from typing import TypeVar
from src.common.models.model import Model

LayerContextType = TypeVar("LayerContextType", bound=SilverContext) 

class BaseModelBuilder(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def _build(self, runtime_context: LayerRuntimeContext[LayerContextType]) -> Model:
        raise NotImplementedError



    def run(self, runtime_context: LayerRuntimeContext[LayerContextType]) -> Model:
        try:
            model = self._build(runtime_context)
            
            if model.data.isEmpty() and model.data.schema.isEmpty():
                return runtime_context.spark.createDataFrame([], schema=model.data.schema) 

            return model

        except Exception as e:
            raise