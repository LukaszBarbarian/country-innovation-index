# src/common/storage_file_builder/base_storage_file_builder.py
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Dict, Any, Tuple
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase

class BaseStorageFileBuilder(ABC):
    def __init__(self, config: ConfigManager):
        self.config = config

    def build_blob_url(self, container_name: str, blob_path: str, account_name: str) -> str:
        """
        Składa pełny URL do bloba w Azure Storage.
        """
        clean_path = blob_path.lstrip("/")
        return f"https://{account_name}.blob.core.windows.net/{container_name.lower()}/{clean_path}"



    @abstractmethod
    def build_file(self,
                          correlation_id: str,
                          container_name: str,
                          storage_account_name: str,
                          **kwargs: Any) -> Dict[str, Any]:
        """
        Metoda abstrakcyjna, która ma za zadanie przygotować wszystkie dane do zapisu,
        włączając ścieżki, metadane i zawartość pliku (jeśli dotyczy).
        """
        pass


    @abstractmethod
    def build_summary_file_output(self,
                            context: ContextBase,
                            results: list,
                            storage_account_name: str,
                            container_name: str,
                            **kwargs: Any) -> Dict[str, Any]:
        pass