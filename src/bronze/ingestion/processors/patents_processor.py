from typing import Any
from src.common.models.base_context import BaseContext
from src.common.enums.domain_source import DomainSource
from src.common.enums.file_format import FileFormat
from src.common.models.processed_result import ProcessedResult
from src.common.processors.base_data_processor import BaseDataProcessor
from src.common.registers.data_processor_registry import DataProcessorRegistry


@DataProcessorRegistry.register(DomainSource.PATENTS)
class PatentsProcessor(BaseDataProcessor):
    def __init__(self):
        super().__init__()

    def process(self, raw_data: Any, context: BaseContext) -> ProcessedResult:
        return ProcessedResult(data=raw_data, format=FileFormat.JSON, skip_upload=True)