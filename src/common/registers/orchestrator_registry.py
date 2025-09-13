from typing import Dict, Type
from src.common.enums.etl_layers import ETLLayer
from src.common.orchestrators.base_orchestrator import BaseOrchestrator
from src.common.registers.base_registry import BaseRegistry


class OrchestratorRegistry(BaseRegistry[ETLLayer, BaseOrchestrator]):
    """
    A registry for mapping ETL layers to their corresponding orchestrator classes.
    
    This class inherits from `BaseRegistry` to provide a consistent way of managing
    and retrieving registered orchestrators based on the ETL layer (e.g., Bronze, Silver, Gold).
    """
    _registry: Dict[ETLLayer, Type[BaseOrchestrator]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[ETLLayer, Type[BaseOrchestrator]]:
        """
        Returns the internal dictionary that stores the registered orchestrator classes.
        This method is required by the base class to access the registry data.
        """
        return cls._registry