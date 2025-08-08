# src/common/ingestion/base_ingestion_strategy.py

from abc import ABC, abstractmethod
from typing import Optional, Any, Dict, List
from src.common.config.config_manager import ConfigManager
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.models.ingestion_result import IngestionResult

class BaseIngestionStrategy(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich strategii pozyskiwania danych.
    Definiuje wspólny interfejs, który muszą zaimplementować konkretne strategie.
    """
    def __init__(self, config: ConfigManager):
        self.config = config

    @abstractmethod
    async def ingest(self, context: BaseLayerContext) -> IngestionResult:
        """
        Abstrakcyjna metoda, która wykonuje operację pozyskiwania danych.
        
        Args:
            context: Obiekt kontekstu warstwy zawierający metadane i konfigurację.

        Returns:
            Obiekt IngestionResult zawierający status operacji i ścieżki do plików.
        """
        pass

    def create_result(
        self,
        status: str,
        message: str,
        output_paths: Optional[List[str]] = None,
        source_response_status_code: Optional[int] = None,
        error_details: Optional[Dict[str, Any]] = None
    ) -> IngestionResult:
        """
        Metoda pomocnicza do tworzenia obiektu IngestionResult.
        """
        return IngestionResult(
            status=status,
            message=message,
            output_paths=output_paths,
            source_response_status_code=source_response_status_code,
            error_details=error_details or {}
        )