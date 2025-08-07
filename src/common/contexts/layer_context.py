# src/common/context/layer_context.py

from dataclasses import dataclass, field
from src.common.enums.domain_source import DomainSource
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from datetime import datetime
from typing import Optional, Any, Dict
from src.common.utils.cache import Cache


@dataclass(frozen=True)
class LayerContext:
    """
    Unified context class for all data processing layers, handling various payloads.
    """
    correlation_id: str
    queue_message_id: str
    etl_layer: ETLLayer
    env: Env
    domain_source: DomainSource
    dataset_name: str
    domain_source_type: DomainSourceType
    
    # Przechowywanie całego payloadu dla elastyczności
    payload: Dict[str, Any] = field(default_factory=dict)
    
    # Przechowywanie payloadu żądania dla konkretnych ustawień źródła
    request_payload: Dict[str, Any] = field(default_factory=dict)

    source_config_payload: Dict[str, Any] = field(default_factory=dict)
    
    # Specyficzne pola, które mogą być puste w zależności od payloadu
    file_path: Optional[str] = None
    ingestion_time_utc: datetime = field(default_factory=datetime.utcnow)
    source_response_status_code: Optional[int] = None
    
    # Cache and other properties
    _cache: Cache = field(default_factory=Cache, repr=False, init=False)

    def __post_init__(self):
        # Metoda wywoływana po inicjalizacji.
        # Sprawdzanie i inicjalizacja pól, które mogą wymagać walidacji lub ustawienia na podstawie innych pól.
        
        # Ustawienie file_path bezpośrednio z payloadu, jeśli istnieje.
        # Użycie object.__setattr__ jest konieczne, ponieważ klasa jest 'frozen'.
        if self.domain_source_type == DomainSourceType.STATIC_FILE:
            file_path_from_payload = self.request_payload.get("file_path")
            object.__setattr__(self, 'file_path', file_path_from_payload)
            if not file_path_from_payload:
                raise ValueError("file_path is required for STATIC_FILE type.")

    @property
    def cache(self) -> Cache:
        return self._cache

    @property
    def empty(self) -> str:
        return "N/A"