# src/common/storage_metadata_file_builder/base_storage_metadata_file_builder.py
from abc import ABC, abstractmethod
import hashlib
import json
from typing import Dict, Any, List
import uuid

from src.common.contexts.base_layer_context import BaseLayerContext
from src.common.models.processed_result import ProcessedResult


class BaseStorageFileBuilder(ABC):
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
    

    def _generate_blob_path_and_name(self, dataset_name: str, correlation_id: str, domain_source: str, file_extension: str, payload_hash: str, ingestion_time_utc: str):
        
        file_name = f"{dataset_name}_{correlation_id}_{payload_hash}.{file_extension}"
        
        blob_path = f"{domain_source}/{ingestion_time_utc}/{file_name}"
        
        return blob_path, file_name
    

    def _compute_payload_hash(self, api_request_payload: Dict[str, Any]) -> str:
        """
        Oblicza hash zapytania na podstawie api_config_payload.
        """
        if not api_request_payload:
            return "nohash"
        normalized_str = json.dumps(api_request_payload, sort_keys=True)
        return hashlib.sha256(normalized_str.encode('utf-8')).hexdigest()[:8]    