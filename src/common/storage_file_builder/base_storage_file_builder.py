# src/common/storage_file_builder/base_storage_file_builder.py
from abc import ABC, abstractmethod
from dataclasses import asdict
import datetime
import hashlib
import json
from typing import Dict, Any, Tuple
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import BaseContext
from src.common.models.manifest import SourceConfigPayload

class BaseStorageFileBuilder(ABC):
    def __init__(self, config: ConfigManager):
        self.config = config

    def build_blob_url(self, container_name: str, blob_path: str, account_name: str) -> str:
        """
        Składa pełny URL do bloba w Azure Storage.
        """
        clean_path = blob_path.lstrip("/")
        return f"https://{account_name}.blob.core.windows.net/{container_name.lower()}/{clean_path}"

    def _compute_hash_name(self, api_request_payload: SourceConfigPayload) -> str:
        """
        Oblicza skrót SHA256 z znormalizowanego payloadu.
        """
        if not api_request_payload:
            return "nohash"
        
        normalized_str = json.dumps(api_request_payload.to_dict(), indent=2)
        return hashlib.sha256(normalized_str.encode("utf-8")).hexdigest()[:8]

    def _get_ingestion_date_path(self, ingestion_time_utc: datetime.datetime) -> str:
        """
        Generuje ścieżkę partycjonowania na podstawie daty.
        """
        return ingestion_time_utc.strftime("%Y/%m/%d")

    @abstractmethod
    def build_file_output(self,
                          context: BaseContext,
                          container_name: str,
                          **kwargs: Any) -> Dict[str, Any]:
        """
        Metoda abstrakcyjna, która ma za zadanie przygotować wszystkie dane do zapisu,
        włączając ścieżki, metadane i zawartość pliku (jeśli dotyczy).
        """
        pass


    @abstractmethod
    def build_summary_file_output(self,
                            context: BaseContext,
                            ingestion_results: list,
                            container_name: str) -> Dict[str, Any]:
        pass