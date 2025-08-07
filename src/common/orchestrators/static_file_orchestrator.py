from typing import Any, List
from src.common.contexts.layer_context import LayerContext
from src.common.enums.domain_source_type import DomainSourceType
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry

@OrchestratorRegistry.register(DomainSourceType.STATIC_FILE)
class StaticFileOrchestrator(BaseOrchestrator):
    async def run(self, context: LayerContext) -> OrchestratorResult:
        if context.file_path:
            all_output_paths: List[str] = [context.file_path]

            return self.create_result(
                context=context,
                status="COMPLETED",
                message="Static file path passed through to Silver layer.",
                all_output_paths=all_output_paths or None
            )
        else:
            return self.create_result(
                context=context,
                status="FAILED",
                message="File path not provided in payload.",
                error_details={"error": "file_path is required for STATIC_FILE type"}
            )        