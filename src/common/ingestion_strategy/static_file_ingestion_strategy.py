import logging
from typing import List
from src.common.contexts.layer_context import LayerContext
from src.common.enums.domain_source_type import DomainSourceType
from src.common.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.models.ingestion_result import IngestionResult
from src.common.registers.ingestion_strategy_registry import IngestionStrategyRegistry

logger = logging.getLogger(__name__)

@IngestionStrategyRegistry.register(DomainSourceType.STATIC_FILE)
class StaticFileIngestionStrategy(BaseIngestionStrategy):
    async def ingest(self, context: LayerContext) -> IngestionResult:
        if not context.file_path:
            logger.error(f"Missing file_path in context for STATIC_FILE type. Correlation ID: {context.correlation_id}")
            return self.create_result(
                status="FAILED",
                message="File path not provided in payload.",
                error_details={"error": "file_path is required for STATIC_FILE type"}
            )
        
        all_output_paths: List[str] = [context.file_path]
        
        return self.create_result(
            status="COMPLETED",
            message="Static file path passed through to the next layer.",
            output_paths=all_output_paths
        )