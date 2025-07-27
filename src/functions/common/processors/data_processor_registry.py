# src/functions/common/processor_registry.py

from src.common.registery.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.functions.common.processors.base_data_processor import BaseDataProcessor

class DataProcessorRegistry(BaseRegistry[DomainSource, BaseDataProcessor]):
    pass
