# src/common/models/file_info.py
from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class FileInfo:
    """
    A dataclass that stores detailed information about a file in Azure Blob Storage.

    This object encapsulates all relevant metadata for a file, including its location,
    size, and various identifiers used for tracking and organization.
    """
    container_name: str = ""
    full_path_in_container: str = ""
    file_name: str = ""
    file_size_bytes: Optional[int] = None
    hash_name: str = ""
    
    domain_source: str = ""
    dataset_name: str = ""
    ingestion_date: str = ""
    correlation_id: Optional[str] = None
    
    full_blob_url: Optional[str] = None
    
    blob_tags: Optional[Dict[str, str]] = None