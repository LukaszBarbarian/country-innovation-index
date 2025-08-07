from typing import Dict, Type
from src.common.enums.domain_source_type import DomainSourceType
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.base_registry import BaseRegistry


class OrchestratorRegistry(BaseRegistry[DomainSourceType, BaseOrchestrator]):
    _registry: Dict[DomainSourceType, Type[BaseOrchestrator]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSourceType, Type[BaseOrchestrator]]:
        return cls._registry