# src/common/models/file_info.py
from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class FileInfo:
    container_name: str = ""
    full_path_in_container: str = ""
    file_name: str = ""
    file_size_bytes: Optional[int] = None
    payload_hash: str = ""
    
    domain_source: str = ""
    dataset_name: str = ""
    ingestion_date: str = ""
    correlation_id: Optional[str] = None 
    
    full_blob_url: Optional[str] = None 
    
    blob_tags: Optional[Dict[str, str]] = None