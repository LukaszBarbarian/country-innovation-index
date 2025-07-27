# src/functions/common/processor_factory.py

from src.functions.common.processors.data_processor_registry import DataProcessorRegistry
from src.functions.common.processors.base_data_processor import BaseDataProcessor
from src.common.enums.domain_source import DomainSource
from src.common.factory.base_factory import BaseFactoryFromRegistry
from src.common.registery.base_registry import BaseRegistry # Dodaj import

class DataProcessorFactory(BaseFactoryFromRegistry[DomainSource, BaseDataProcessor]):
    @classmethod
    def get_registry(cls) -> BaseRegistry:
        return DataProcessorRegistry
    