# src/common/storage_metadata_file_builder/base_storage_metadata_file_builder.py
from abc import ABC, abstractmethod
import hashlib
import json
from typing import Dict, Any, List
import uuid

from src.common.config.config_manager import ConfigManager
from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.models.processed_result import ProcessedResult


class BaseStorageFileBuilder(ABC):
    def __init__(self, config: ConfigManager):
        self.config = config

    @abstractmethod
    def build_file_output(self, 
                          processed_records_results: ProcessedResult, 
                          context: BaseLayerContext,
                          container_name: str) -> Dict[str, Any]:
        """
        Przyjmuje listę obiektów ProcessedResult i kontekst ingestii.
        Tworzy finalny string JSON (lub bajty dla innych formatów),
        generuje nazwę pliku/ścieżkę i tworzy obiekt file_info wraz z tagami bloba.
        Zwraca słownik zawierający 'file_content_bytes' oraz 'file_info'.
        """
        pass
    
    def build_blob_url(self, container_name: str, blob_path: str, account_name: str) -> str:
        """
        Składa pełny URL do bloba w Azure Storage.
        Obsługuje zarówno ścieżki generowane, jak i podawane ręcznie (z / lub bez).
        """
        clean_path = blob_path.lstrip("/")
        return f"https://{account_name}.blob.core.windows.net/{container_name}/{clean_path}"


    def _generate_blob_path_and_name(self, 
                                     dataset_name: str,
                                     correlation_id: str, 
                                     domain_source: str, 
                                     file_extension: str, 
                                     payload_hash: str, 
                                     ingestion_time_utc: str):
        file_name = f"{dataset_name}_{correlation_id}_{payload_hash}.{file_extension}"
        blob_path = f"{domain_source}/{ingestion_time_utc}/{file_name}"
        return blob_path, file_name

    def _compute_payload_hash(self, api_request_payload: Dict[str, Any]) -> str:
        if not api_request_payload:
            return "nohash"
        normalized_str = json.dumps(api_request_payload, sort_keys=True)
        return hashlib.sha256(normalized_str.encode("utf-8")).hexdigest()[:8]