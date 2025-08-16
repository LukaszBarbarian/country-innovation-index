# src/silver/parsers/ingestion_summary_parser.py
from ctypes import Union
from typing import Dict, Any
import json

from src.common.models.ingestions import IngestionResult, IngestionSummary

class IngestionSummaryParser:
    def parse(self, summary_data: Union[str, Dict[str, Any]]) -> IngestionSummary:
        """
        Parsuje surowe dane (JSON string lub słownik) na obiekt IngestionSummary.
        """
        if isinstance(summary_data, str):
            summary_data = json.loads(summary_data)
        
        try:
            results_list = [IngestionResult(**res) for res in summary_data.get('results', [])]
            return IngestionSummary(
                status=summary_data['status'],
                env=summary_data['env'],
                layer_name=summary_data['layer_name'],
                correlation_id=summary_data['correlation_id'],
                timestamp=summary_data['timestamp'],
                processed_items=summary_data['processed_items'],
                results=results_list
            )
        except (KeyError, ValueError) as e:
            raise ValueError(f"Błąd parsowania IngestionSummary: {e}")