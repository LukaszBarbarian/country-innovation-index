# src/common/models/file_info.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class FileInfo:
    """
    Klasa przechowująca kompleksowe informacje o zapisanym pliku blob w Azure Storage.
    """
    container_name: str
    full_path_in_container: str
    file_name: str
    storage_account_name: str
    file_size_bytes: Optional[int] = None
    url: Optional[str] = None

    def __post_init__(self):
        # Automatycznie generuj URL, jeśli nie został podany i wszystkie potrzebne dane są dostępne
        if self.url is None and all([self.storage_account_name, self.container_name, self.full_path_in_container]):
            self.url = f"https://{self.storage_account_name}.blob.core.windows.net/{self.container_name}/{self.full_path_in_container}"