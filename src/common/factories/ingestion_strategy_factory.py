
from src.common.enums.domain_source_type import DomainSourceType
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.registers.ingestion_strategy_registry import IngestionStrategyRegistry


class IngestionStrategyFactory(BaseFactoryFromRegistry[DomainSourceType, BaseIngestionStrategy]):
    @classmethod
    def get_registry(cls):
        """
        Zwraca instancję DataReaderRegistry.
        """
        return IngestionStrategyRegistry()