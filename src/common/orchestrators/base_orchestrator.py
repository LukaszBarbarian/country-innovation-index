# src/common/orchestrators/base_orchestrator.py
import asyncio
import logging
import datetime
from abc import ABC, abstractmethod
from typing import List, Any

from src.common.config.config_manager import ConfigManager
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.models.base_context import ContextBase
from src.common.orchestrators.orchestrator_result_builder import OrchestratorResultBuilder
from src.common.models.base_process_result import BaseProcessResult

logger = logging.getLogger(__name__)

class BaseOrchestrator(ABC):
    def __init__(self, config: ConfigManager, spark=None):
        self.config = config
        self.spark = spark

    async def execute(self, context: ContextBase) -> OrchestratorResult:
        """Główna metoda do uruchamiania orkiestratora z obsługą błędów."""
        start_time = datetime.datetime.utcnow()
        result_builder = OrchestratorResultBuilder(context, self.config)

        try:
            raw_results = await self.run(context)
            processed_results = []
            
            # Sprawdza każdy wynik w poszukiwaniu wyjątków
            for result in raw_results:
                if isinstance(result, Exception):
                    # Jeśli jest wyjątek, rzuca go, aby obsłużył go blok except
                    raise result
                processed_results.append(result)
            
            for result in processed_results:
                result_builder.add_result(result)
            
            return await result_builder.build_and_save()
            
        except Exception as e:
            duration_ms = int((datetime.datetime.utcnow() - start_time).total_seconds() * 1000)
            logger.exception(f"Orchestrator for {context.etl_layer.value} failed with a critical error.")
            return result_builder.create_error_orchestrator_result(e, duration_ms)

    @abstractmethod
    async def run(self, context: ContextBase) -> List[BaseProcessResult]:
        """
        Metoda abstrakcyjna, która musi być zaimplementowana przez każdą podklasę.
        Zawiera specyficzną logikę warstwy.
        Zwraca listę obiektów dziedziczących z BaseProcessResult.
        """
        pass