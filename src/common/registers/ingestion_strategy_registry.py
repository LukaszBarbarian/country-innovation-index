
from typing import Dict, Type
from src.common.enums.domain_source_type import DomainSourceType
from src.bronze.ingestion.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.registers.base_registry import BaseRegistry


class IngestionStrategyRegistry(BaseRegistry[DomainSourceType, BaseIngestionStrategy]):
    _registry: Dict[DomainSourceType, Type[BaseIngestionStrategy]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSourceType, Type[BaseIngestionStrategy]]:
        return cls._registry