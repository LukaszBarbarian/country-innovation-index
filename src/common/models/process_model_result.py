from dataclasses import dataclass
from typing import List, Optional

from src.common.models.base_process_result import BaseProcessResult


@dataclass
class ProcessModelResult(BaseProcessResult):
    """
    Wynik przetwarzania modelu dla warstw Silver i Gold.
    Dziedziczy z BaseProcessResult, dodając specyficzne pola.
    """
    model: Optional[str] = None
    operation_type: Optional[str] = None
    output_path: Optional[str] = None
    
