from src.common.enums.domain_source_type import DomainSourceType
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.bronze.ingestion.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.registers.ingestion_strategy_registry import IngestionStrategyRegistry


class IngestionStrategyFactory(BaseFactoryFromRegistry[DomainSourceType, BaseIngestionStrategy]):
    """
    A factory for creating ingestion strategy instances.
    
    This class inherits from `BaseFactoryFromRegistry` and is responsible for
    retrieving and instantiating the correct ingestion strategy based on the
    `DomainSourceType`.
    """
    @classmethod
    def get_registry(cls):
        """
        Returns the registry for ingestion strategies.
        
        This method provides the `IngestionStrategyRegistry` instance, which
        is used by the base factory to map domain source types to their
        respective ingestion strategy classes.
        """
        return IngestionStrategyRegistry()