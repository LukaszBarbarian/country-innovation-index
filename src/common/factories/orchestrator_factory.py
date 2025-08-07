
from typing import Type
from src.common.enums.domain_source_type import DomainSourceType
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry


class OrchestratorFactory(BaseFactoryFromRegistry[DomainSourceType, BaseOrchestrator]):
    @classmethod
    def get_registry(cls):
        """
        Zwraca instancję DataReaderRegistry.
        """
        return OrchestratorRegistry()