# src/functions/common/processor_factory.py

from src.common.registers.data_processor_registry import DataProcessorRegistry
from src.common.processors.base_data_processor import BaseDataProcessor
from src.common.enums.domain_source import DomainSource
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.registers.base_registry import BaseRegistry

from src.bronze.ingestion.processors.nobelprize_processor import NobelPrizeProcessor


class DataProcessorFactory(BaseFactoryFromRegistry[DomainSource, BaseDataProcessor]):
    """
    A factory for creating instances of data processors.

    This class extends `BaseFactoryFromRegistry` and uses `DataProcessorRegistry`
    to look up and instantiate the correct processor class based on a `DomainSource` key.
    """
    @classmethod
    def get_registry(cls) -> BaseRegistry:
        """
        Retrieves the specific registry for data processor classes.

        This method is required by the abstract base class and provides the
        registry that maps a DomainSource to its corresponding BaseDataProcessor.
        
        Returns:
            BaseRegistry: The `DataProcessorRegistry` instance.
        """
        return DataProcessorRegistry()