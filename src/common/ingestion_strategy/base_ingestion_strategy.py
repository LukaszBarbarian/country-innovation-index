# src/common/ingestion/base_ingestion_strategy.py

from abc import ABC, abstractmethod
from typing import Optional, Any, Dict, List
from src.common.config.config_manager import ConfigManager
from src.common.models.ingestion_context import IngestionContext
from src.common.models.ingestion_result import IngestionResult

class BaseIngestionStrategy(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich strategii pozyskiwania danych.
    Definiuje wspólny interfejs, który muszą zaimplementować konkretne strategie.
    """
    def __init__(self, config: ConfigManager):
        self.config = config

    @abstractmethod
    async def ingest(self, context: IngestionContext) -> IngestionResult:
        """
        Abstrakcyjna metoda, która wykonuje operację pozyskiwania danych.
        """
        pass