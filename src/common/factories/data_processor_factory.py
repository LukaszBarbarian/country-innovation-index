# src/functions/common/processor_factory.py

from src.common.registers.data_processor_registry import DataProcessorRegistry
from src.common.processors.base_data_processor import BaseDataProcessor
from src.common.enums.domain_source import DomainSource
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.registers.base_registry import BaseRegistry

from src.functions.ingestion.processors.nobelprize_processor import NobelPrizeProcessor


class DataProcessorFactory(BaseFactoryFromRegistry[DomainSource, BaseDataProcessor]):
    @classmethod
    def get_registry(cls) -> BaseRegistry:
        return DataProcessorRegistry()
    