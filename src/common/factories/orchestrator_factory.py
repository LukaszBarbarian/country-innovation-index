from src.common.enums.etl_layers import ETLLayer
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.orchestrator_registry import OrchestratorRegistry


class OrchestratorFactory(BaseFactoryFromRegistry[ETLLayer, BaseOrchestrator]):
    """
    A factory for creating instances of orchestrators based on the ETL layer.

    This class extends the generic `BaseFactoryFromRegistry` to provide a dedicated
    mechanism for instantiating the correct orchestrator for a given ETL layer
    (e.g., Bronze, Silver, Gold).
    """
    @classmethod
    def get_registry(cls):
        """
        Returns the registry containing the mapping of ETL layers to orchestrator classes.
        
        This method is a required part of the factory pattern, connecting the factory
        to its specific registry.
        """
        return OrchestratorRegistry()