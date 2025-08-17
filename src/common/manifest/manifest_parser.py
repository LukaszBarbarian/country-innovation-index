from abc import ABC, abstractmethod
from typing import Dict
from src.common.models.manifest import ManifestBase


class BaseManifestParser(ABC):
    @staticmethod
    @abstractmethod
    def parse(manifest_data: Dict) -> ManifestBase:
        """Parsowanie manifestu do konkretnej klasy dataclass"""
        pass