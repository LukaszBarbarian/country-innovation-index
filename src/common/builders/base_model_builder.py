# src/silver/model_builders/base_model_builder.py (bez zmian w tej klasie względem ostatniej propozycji)

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from src.common.contexts.layer_runtime_context import LayerRuntimeContext
from src.silver.contexts.silver_context import SilverContext
from typing import TypeVar

LayerContextType = TypeVar("LayerContextType", bound=SilverContext) 

class BaseModelBuilder(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def build_model(self, runtime_context: LayerRuntimeContext[LayerContextType]) -> DataFrame:
        pass

    def run(self, runtime_context: LayerRuntimeContext[LayerContextType]) -> DataFrame:
        try:
            model_df = self.build_model(runtime_context) 
            
            if model_df.isEmpty() and model_df.schema.isEmpty():
                return runtime_context.spark.createDataFrame([], schema=model_df.schema) 

            return model_df

        except Exception as e:
            raise