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
    """
    An abstract base class for all data pipeline orchestrators.
    This class defines the fundamental structure for running a pipeline process,
    handling common tasks such as error management, timing, and result aggregation.
    """
    def __init__(self, config: ConfigManager, spark=None):
        """
        Initializes the orchestrator with configuration and an optional Spark session.

        Args:
            config (ConfigManager): An instance of the configuration manager.
            spark (Any, optional): An optional Spark session object. Defaults to None.
        """
        self.config = config
        self.spark = spark

    async def execute(self, context: ContextBase) -> OrchestratorResult:
        """
        The main method to run the orchestrator with robust error handling.

        This method orchestrates the execution of the pipeline, delegating the
        layer-specific logic to the `run` method. It captures the start time,
        runs the `run` method, and then processes the results. It is responsible
        for:
        1.  Catching any exceptions raised during the execution.
        2.  Aggregating results from all parallel tasks.
        3.  Building a comprehensive `OrchestratorResult` object to summarize
            the entire process, including successes, failures, and duration.
        4.  Saving the final result object.

        Args:
            context (ContextBase): The context object for the current pipeline run.

        Returns:
            OrchestratorResult: A summary object detailing the outcome of the execution.
        """
        start_time = datetime.datetime.now(datetime.timezone.utc)
        result_builder = OrchestratorResultBuilder(context, self.config)

        try:
            raw_results = await self.run(context)
            processed_results = []
            
            for result in raw_results:
                if isinstance(result, Exception):
                    raise result
                processed_results.append(result)
            
            for result in processed_results:
                result_builder.add_result(result)
            
            return await result_builder.build_and_save()
            
        except Exception as e:
            duration_ms = int((datetime.datetime.now(datetime.timezone.utc) - start_time).total_seconds() * 1000)
            logger.exception(f"Orchestrator for {context.etl_layer.value} failed with a critical error.")
            return result_builder.create_error_orchestrator_result(e, duration_ms)

    @abstractmethod
    async def run(self, context: ContextBase) -> List[BaseProcessResult]:
        """
        An abstract method that must be implemented by each subclass.

        This method contains the specific, layer-dependent orchestration logic.
        It is responsible for initiating and awaiting the main tasks for that
        particular pipeline layer (e.g., Bronze, Silver, Gold).

        Args:
            context (ContextBase): The context object for the current pipeline run.

        Returns:
            List[BaseProcessResult]: A list of results from the individual tasks
                                     executed by the orchestrator.
        """
        pass