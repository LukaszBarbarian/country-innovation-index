from abc import ABC, abstractmethod
from typing import Any, Dict
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.models.processed_result import ProcessedResult


class BaseDataProcessor(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def process(self, raw_data: Any, context: BaseLayerContext) -> ProcessedResult:
        pass
