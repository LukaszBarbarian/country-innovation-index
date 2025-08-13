# src/common/contexts/silver_layer_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional

# Importujemy nowy model wyników
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.enums.domain_source import DomainSource
from src.silver.context.silver_result_context import SilverResultContext


@dataclass(frozen=True, kw_only=True)
class SilverLayerContext(BaseLayerContext):
    # Dodajemy listę wyników
    results: List[SilverResultContext] = field(default_factory=list)
    
    # Pozostałe opcjonalne pola specyficzne dla Silver
    status: str
    message: Optional[str] = None
    source_response_status_code: Optional[int] = None
    error_details: Optional[Dict[str, Any]] = None




    def get_result_by_domain_source(self, domain_source: DomainSource) -> List[Optional[SilverResultContext]]:
        """
        Zwraca konkretny obiekt SilverResultContext dla podanego źródła domeny.
        Zwraca None, jeśli walidacja nie powiodła się lub jeśli źródło nie istnieje.
        """
        results: List[Optional[SilverResultContext]] = []

        if not self.is_valid:
            return None

        for result in self.results:
            if result.domain_source == domain_source:
                results.append(result)
        
        return results