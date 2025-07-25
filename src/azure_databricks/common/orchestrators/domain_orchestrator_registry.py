# azure_databricks/factories/domain_orchestrator_registry.py

from typing import Dict, Type, Any, Callable, List
from src.common.enums.domain_source import DomainSource
from src.common.enums.etl_layers import ETLLayer # Import ETLLayer
from src.azure_databricks.common.orchestrators.base_domain_orchestrator import BaseDomainOrchestrator

class DomainOrchestratorRegistry:
    _registry: Dict[ETLLayer, Dict[DomainSource, Type[BaseDomainOrchestrator]]] = {}

    @classmethod
    def _ensure_layer_exists(cls, etl_layer: ETLLayer):
        if etl_layer not in cls._registry:
            cls._registry[etl_layer] = {}

    @classmethod
    def _register(cls, 
                  domain_source: DomainSource, 
                  orchestrator_class: Type[BaseDomainOrchestrator], 
                  applicable_layers: List[ETLLayer]): # Teraz lista warstw
        
        if not issubclass(orchestrator_class, BaseDomainOrchestrator):
            raise TypeError(f"Klasa {orchestrator_class.__name__} musi dziedziczyć z BaseDomainOrchestrator.")
        
        for layer in applicable_layers:
            cls._ensure_layer_exists(layer)
            if domain_source in cls._registry[layer]:
                print(f"Ostrzeżenie: Domenowy orkiestrator '{domain_source.value}' dla warstwy '{layer.value}' jest już zarejestrowany. Zostanie nadpisany.")
            cls._registry[layer][domain_source] = orchestrator_class
            print(f"Zarejestrowano domenowy orkiestrator '{domain_source.value}' dla warstwy '{layer.value}'")

    @classmethod
    def register_orchestrator_decorator(cls, domain_source: DomainSource, applicable_layers: List[ETLLayer]) -> Callable[[Type[BaseDomainOrchestrator]], Type[BaseDomainOrchestrator]]:
        """
        Dekorator do automatycznej rejestracji klas domenowych orkiestratorów.
        
        Args:
            domain_source (DomainSource): Unikalny identyfikator domeny (np. WHO, WORLD_BANK).
            applicable_layers (List[ETLLayer]): Lista warstw ETL, dla których ten orkiestrator jest aktywny.
        
        Returns:
            Callable: Dekorator, który przyjmuje klasę domenowego orkiestratora i ją rejestruje.
        """
        def decorator(orchestrator_class: Type[BaseDomainOrchestrator]) -> Type[BaseDomainOrchestrator]:
            cls._register(domain_source, orchestrator_class, applicable_layers)
            return orchestrator_class
        return decorator

    @classmethod
    def get_orchestrator_class_for_layer(cls, etl_layer: ETLLayer, domain_source: DomainSource) -> Type[BaseDomainOrchestrator]:
        """Pobiera klasę orkiestratora dla danej warstwy i domeny."""
        if etl_layer not in cls._registry:
            raise ValueError(f"Brak zarejestrowanych orkiestratorów dla warstwy '{etl_layer.value}'.")
        
        orchestrator_class = cls._registry[etl_layer].get(domain_source)
        if not orchestrator_class:
            raise ValueError(f"Domenowy orkiestrator dla '{domain_source.value}' nie jest zarejestrowany dla warstwy '{etl_layer.value}'.")
        return orchestrator_class

    @classmethod
    def get_all_registered_orchestrators_for_layer(cls, etl_layer: ETLLayer) -> Dict[DomainSource, Type[BaseDomainOrchestrator]]:
        """Zwraca wszystkie zarejestrowane orkiestatory dla danej warstwy ETL."""
        return cls._registry.get(etl_layer, {})

    @classmethod
    def get_all_registered_orchestrators(cls) -> Dict[ETLLayer, Dict[DomainSource, Type[BaseDomainOrchestrator]]]:
        """Zwraca wszystkie zarejestrowane orkiestatory dla wszystkich warstw."""
        return cls._registry