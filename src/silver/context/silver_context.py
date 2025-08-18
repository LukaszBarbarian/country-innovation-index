# src/common/contexts/silver_layer_context.py

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional

# Importujemy nowy model wyników
from src.common.enums.domain_source import DomainSource
from src.common.models.base_context import BaseContext
from src.common.models.ingestions import IngestionResult, IngestionSummary
from src.common.models.manifest import ManualDataPath, Model


@dataclass(frozen=True, kw_only=True)
class SilverLayerContext(BaseContext):
    references_tables: Dict[str, str]
    models: List[Model]
    manual_data_paths: List[ManualDataPath]
    ingestion_summary: IngestionSummary


    def get_ingestion_results_for_domain_source(self, domain_source: DomainSource) -> Optional[List[IngestionResult]]:
        """
        Zwraca listę obiektów IngestionResult dla podanego domain_source
        pobraną z IngestionSummary w kontekście.
        """
        if not self.ingestion_summary:
            return None
        
        # Iteruj po wynikach i zbieraj tylko te, które są 'valid' i pasują do domain_source
        results = [
            result for result in self.ingestion_summary.results
            if result.domain_source == domain_source.name and result.is_valid
        ]
        
        if not results:
            return None
        
        return results