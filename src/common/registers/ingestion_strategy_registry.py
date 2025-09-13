from typing import Dict, Type
from src.common.enums.domain_source_type import DomainSourceType
from src.bronze.ingestion.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.registers.base_registry import BaseRegistry


class IngestionStrategyRegistry(BaseRegistry[DomainSourceType, BaseIngestionStrategy]):
    """
    A registry for mapping domain source types to their corresponding ingestion strategy classes.
    
    This class inherits from `BaseRegistry` to provide a consistent mechanism for
    managing and retrieving different ingestion strategies based on the source type (e.g., API, database, file).
    """
    _registry: Dict[DomainSourceType, Type[BaseIngestionStrategy]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSourceType, Type[BaseIngestionStrategy]]:
        """
        Returns the internal dictionary that stores the registered ingestion strategy classes.
        
        This method is required by the base class to provide access to the registry data.
        """
        return cls._registry