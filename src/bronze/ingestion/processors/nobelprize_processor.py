from src.common.models.base_context import ContextBase
from src.common.processors.base_data_processor import BaseDataProcessor
from src.common.registers.data_processor_registry import DataProcessorRegistry
from src.common.enums.domain_source import DomainSource
from typing import Any, Dict
from src.common.models.processed_result import ProcessedResult
from src.common.enums.file_format import FileFormat

@DataProcessorRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeProcessor(BaseDataProcessor):
    """
    A data processor for Nobel Prize data.
    
    This class is registered to handle data from the `DomainSource.NOBELPRIZE`.
    It implements the `BaseDataProcessor` to standardize data processing.
    """
    def process(self, raw_data: Any, context: ContextBase) -> ProcessedResult:
        """
        Processes raw data from the Nobel Prize API.

        This method takes raw data, makes a copy, and wraps it in a `ProcessedResult` object.
        It explicitly sets the output file format to `FileFormat.JSON`.

        Args:
            raw_data (Any): The raw data to be processed, typically a dictionary
                            or list of dictionaries.
            context (ContextBase): The context object for the current process,
                                   containing metadata and state.

        Returns:
            ProcessedResult: An object containing the processed data and its format.
        """
        return ProcessedResult(data=raw_data.copy(), format=FileFormat.JSON)