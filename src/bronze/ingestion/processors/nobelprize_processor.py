from src.common.processors.base_data_processor import BaseDataProcessor
from src.common.registers.data_processor_registry import DataProcessorRegistry
from src.common.enums.domain_source import DomainSource
from typing import Any, Dict
from src.common.contexts.bronze_context import BronzeContext
from src.common.models.processed_result import ProcessedResult
from src.common.enums.file_format import FileFormat

@DataProcessorRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeProcessor(BaseDataProcessor):
    def process(self, raw_data: Dict[str, Any], context: BronzeContext):
        return ProcessedResult(data=raw_data.copy(), format=FileFormat.JSON)