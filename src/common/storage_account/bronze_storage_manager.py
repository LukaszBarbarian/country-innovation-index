# src/common/storage/bronze_storage_manager.py

from __future__ import annotations # Umożliwia użycie typów w adnotacjach, które są definiowane później
import json
from datetime import datetime
from typing import Any, Union, Dict, Literal # Dodane Literal dla typowania metody download_blob

# WAŻNE: Upewnij się, że ta ścieżka importu jest poprawna w Twoim projekcie.
# Zakładam, że StorageManagerBase znajduje się obok BronzeStorageManager
from src.common.storage_account.storage_manager import StorageManagerBase
import logging 
logger = logging.getLogger(__name__)


class BronzeStorageManager(StorageManagerBase):
    """
    Menedżer do interakcji z kontenerem 'bronze' w Azure Blob Storage.
    Odpowiedzialny za generyczne operacje wgrywania i pobierania blobów.
    """
    def __init__(self):
        super().__init__("bronze")
        logger.info(f"BronzeStorageManager initialized for container: {self.container_name}") 

    def upload_blob(self, 
                    data_content: Union[Dict[str, Any], str, bytes], 
                    blob_name: str,    
                    overwrite: bool = True
                   ) -> str:
        """
        Wgrywa zawartość danych do określonej ścieżki bloba w warstwie Bronze.
        
        Args:
            data_content (Union[Dict[str, Any], str, bytes]): Dane do wgrania.
                                                               Jeśli `dict`, zostanie zserializowany do JSON.
                                                               Jeśli `str`, zostanie zakodowany do UTF-8.
            blob_name (str): Pełna ścieżka do bloba w kontenerze (np. "api_name/rok/miesiac/dzien/plik.json").
            overwrite (bool): Czy nadpisać istniejący blob.

        Returns:
            str: Pełna ścieżka do bloba (nazwa_kontenera/nazwa_bloba).
        
        Raises:
            TypeError: Jeśli typ data_content jest nieobsługiwany.
            Exception: Jeśli wystąpi błąd podczas operacji na storage.
        """
        
        # Konwersja danych do bajtów, jeśli to konieczne
        if isinstance(data_content, dict): 
            content_to_upload = json.dumps(data_content, indent=2).encode('utf-8')
        elif isinstance(data_content, str):
            content_to_upload = data_content.encode('utf-8')
        elif isinstance(data_content, bytes):
            content_to_upload = data_content
        else:
            logger.error(f"Nieobsługiwany typ danych do wgrania: {type(data_content)}. Musi być dict, str lub bytes.")
            raise TypeError(f"Nieobsługiwany typ danych do wgrania: {type(data_content)}. Musi być dict, str lub bytes.")

        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(content_to_upload, overwrite=overwrite)
            
            logger.info(f"Wgrano dane do {self.container_name}/{blob_name}")
            return f"{self.container_name}/{blob_name}"
        except Exception as e:
            logger.error(f"Błąd podczas wgrywania bloba '{blob_name}': {e}")
            raise 

    def download_blob(self, blob_path: str, decode_as: Union[None, Literal['text'], Literal['json']] = None) -> Any:
        """
        Pobiera dane z bloba.
        
        Args:
            blob_path (str): Pełna ścieżka do bloba w kontenerze.
            decode_as (Union[None, Literal['text'], Literal['json']]): Sposób dekodowania danych.
                - None: zwraca surowe bajty (bytes).
                - 'text': zwraca string (dekodowany jako UTF-8).
                - 'json': próbuje sparsować jako JSON.
        
        Returns:
            Any: Dekodowane dane (dict, str) lub surowe bajty (bytes).
        
        Raises:
            ValueError: Jeśli nie uda się zdekodować JSON.
            Exception: Jeśli wystąpi błąd podczas operacji na storage.
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_path)
            download_stream = blob_client.download_blob()
            data_bytes = download_stream.readall()
            
            if decode_as == 'json':
                return json.loads(data_bytes)
            elif decode_as == 'text':
                return data_bytes.decode('utf-8')
            return data_bytes 
        except Exception as e:
            logger.error(f"Błąd podczas pobierania bloba '{blob_path}': {e}")
            raise