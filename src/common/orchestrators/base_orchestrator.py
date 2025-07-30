# common/orchestrators/base_orchestrator.py
from abc import ABC, abstractmethod
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.config.config_manager import ConfigManager


class BaseOrchestrator(ABC):
    def __init__(self, config: ConfigManager):
        self.config = config
        self.storage_account_name = self.config.get_setting("DATA_LAKE_STORAGE_ACCOUNT_NAME")

    @abstractmethod
    async def run(self, context: BaseLayerContext) -> OrchestratorResult:
        pass