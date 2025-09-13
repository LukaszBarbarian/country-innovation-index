from abc import ABC, abstractmethod
from typing import Any, Dict
from src.common.models.base_context import ContextBase
from src.common.models.processed_result import ProcessedResult


class BaseDataProcessor(ABC):
    """
    An abstract base class for data processors.

    This class serves as a contract, ensuring that any class that inherits
    from it will have a 'process' method for transforming raw data into a
    standardized format.
    """
    def __init__(self):
        """
        Initializes the data processor.
        """
        pass

    @abstractmethod
    def process(self, raw_data: Any, context: ContextBase) -> ProcessedResult:
        """
        An abstract method to process raw data.

        Subclasses must implement this method to define their specific
        data transformation logic.

        Args:
            raw_data (Any): The raw data to be processed.
            context (ContextBase): The context of the current process, containing
                                   relevant metadata.

        Returns:
            ProcessedResult: The result of the processing, packaged in a
                             standardized object.
        """
        pass