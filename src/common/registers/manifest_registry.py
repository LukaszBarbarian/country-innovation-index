# Używamy BaseRegistry, którą już masz
from src.common.enums.etl_layers import ETLLayer
from src.common.models.manifest import ManifestBase
from src.common.registers.base_registry import BaseRegistry
from typing import Dict, Type

class ManifestParserRegistry(BaseRegistry[ETLLayer, ManifestBase]):
    _registry: Dict[ETLLayer, Type[ManifestBase]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[ETLLayer, Type[ManifestBase]]:
        return cls._registry