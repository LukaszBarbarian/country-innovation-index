from typing import Dict, Type
from src.common.enums.etl_layers import ETLLayer
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.base_registry import BaseRegistry


class OrchestratorRegistry(BaseRegistry[ETLLayer, BaseOrchestrator]):
    _registry: Dict[ETLLayer, Type[BaseOrchestrator]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[ETLLayer, Type[BaseOrchestrator]]:
        return cls._registry