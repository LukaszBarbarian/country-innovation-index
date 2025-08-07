
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry


class OrchestratorFactory(BaseFactoryFromRegistry[ETLLayer, BaseOrchestrator]):
    @classmethod
    def get_registry(cls):
        """
        Zwraca instancję DataReaderRegistry.
        """
        return OrchestratorRegistry()