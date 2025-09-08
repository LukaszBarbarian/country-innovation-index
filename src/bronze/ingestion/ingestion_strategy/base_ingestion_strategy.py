# src/common/ingestion/base_ingestion_strategy.py

from abc import ABC, abstractmethod
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.models.manifest import BronzeManifestSource
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase
from src.common.models.ingestion_result import IngestionResult

class BaseIngestionStrategy(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich strategii pozyskiwania danych.
    Definiuje wspólny interfejs, który muszą zaimplementować konkretne strategie.
    """
    def __init__(self, config: ConfigManager, context: ContextBase):
        self.config = config
        self.context = context

    @abstractmethod
    async def ingest(self, source: BronzeManifestSource) -> IngestionResult:
        """
        Abstrakcyjna metoda, która wykonuje operację pozyskiwania danych.
        """
        pass