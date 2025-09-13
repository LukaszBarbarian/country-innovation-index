from typing import Any
from src.common.models.base_context import ContextBase
from src.common.processors.base_data_processor import BaseDataProcessor
from src.common.registers.data_processor_registry import DataProcessorRegistry
from src.common.enums.domain_source import DomainSource
from typing import Any, Dict
from src.common.models.processed_result import ProcessedResult
from src.common.enums.file_format import FileFormat

@DataProcessorRegistry.register(DomainSource.WORLDBANK)
class WorldbankProcessor(BaseDataProcessor):
    """
    A data processor for handling data from the World Bank API.

    This class is registered with `DataProcessorRegistry` to specifically handle
    data originating from `DomainSource.WORLDBANK`. It extends the
    `BaseDataProcessor` to maintain a consistent interface for data processing tasks.
    """
    def process(self, raw_data: Any, context: ContextBase) -> ProcessedResult:
        """
        Processes the raw data fetched from the World Bank API.

        This method takes the raw data, creates a copy to ensure immutability,
        and wraps it into a `ProcessedResult` object. It specifies that the data
        is to be stored in `FileFormat.JSON`. The processing logic here is straightforward,
        primarily focused on preparing the data for the next stage of the pipeline
        without complex transformations.

        Args:
            raw_data (Any): The raw data from the World Bank API.
            context (ContextBase): The context object for the current data pipeline run.

        Returns:
            ProcessedResult: An object containing the processed data and the target file format.
        """
        return ProcessedResult(data=raw_data.copy(), format=FileFormat.JSON)