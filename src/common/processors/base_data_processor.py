from abc import ABC, abstractmethod
from typing import Any, Dict
from src.common.models.base_context import ContextBase
from src.common.models.processed_result import ProcessedResult


class BaseDataProcessor(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def process(self, raw_data: Any, context: ContextBase) -> ProcessedResult:
        pass
