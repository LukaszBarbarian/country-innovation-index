from src.common.models.base_context import BaseContext
from src.common.processors.base_data_processor import BaseDataProcessor
from src.common.registers.data_processor_registry import DataProcessorRegistry
from src.common.enums.domain_source import DomainSource
from typing import Any, Dict
from src.common.models.processed_result import ProcessedResult
from src.common.enums.file_format import FileFormat

@DataProcessorRegistry.register(DomainSource.WORLDBANK)
class WorldbankProcessor(BaseDataProcessor):
    def process(self, raw_data: Any, context: BaseContext) -> ProcessedResult:
        return ProcessedResult(data=raw_data.copy(), format=FileFormat.JSON)