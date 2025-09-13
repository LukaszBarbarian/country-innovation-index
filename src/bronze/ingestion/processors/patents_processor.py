from typing import Any
from src.common.models.base_context import ContextBase
from src.common.enums.domain_source import DomainSource
from src.common.enums.file_format import FileFormat
from src.common.models.processed_result import ProcessedResult
from src.common.processors.base_data_processor import BaseDataProcessor
from src.common.registers.data_processor_registry import DataProcessorRegistry


@DataProcessorRegistry.register(DomainSource.PATENTS)
class PatentsProcessor(BaseDataProcessor):
    """
    A data processor for handling data related to patents.
    
    This class is specifically designed to process data from the `DomainSource.PATENTS`.
    It inherits from `BaseDataProcessor` to ensure a consistent interface within the
    data processing pipeline.
    """
    def process(self, raw_data: Any, context: ContextBase) -> ProcessedResult:
        """
        Processes raw patent data and returns a result object.

        This method takes raw data, such as a dictionary or a list, and wraps it
        in a `ProcessedResult` object. The key feature of this processor is that
        it sets the `skip_upload` flag to `True`, indicating that the data should
        not be uploaded to storage, which is a common requirement for raw data
        sources that are only meant for local processing or for passing to another
        stage without an intermediate storage step.

        Args:
            raw_data (Any): The raw data to be processed.
            context (ContextBase): The context object for the current process.

        Returns:
            ProcessedResult: An object containing the processed data, its format (`FileFormat.JSON`),
                             and a flag to skip the upload to storage.
        """
        return ProcessedResult(data=raw_data, format=FileFormat.JSON, skip_upload=True)