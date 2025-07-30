# src/common/models/file_info.py
from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class FileInfo:
    container_name: str = ""
    full_path_in_container: str = ""
    file_name: str = ""
    storage_account_name: str = ""
    file_size_bytes: Optional[int] = None
    url: Optional[str] = None
    
    domain_source: str = ""
    dataset_name: str = ""
    ingestion_date: str = ""
    correlation_id: Optional[str] = None 
    
    # NOWE: Pole na tagi bloba
    blob_tags: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.url is None and all([self.storage_account_name, self.container_name, self.full_path_in_container]):
            normalized_path = self.full_path_in_container.lstrip('/')
            self.url = f"https://{self.storage_account_name}.blob.core.windows.net/{self.container_name}/{normalized_path}"