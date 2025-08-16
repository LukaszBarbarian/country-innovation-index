
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.base_factory import BaseFactoryFromRegistry

from src.common.manifest.manifest_parser import BaseManifestParser
from src.common.registers.manifest_registry import ManifestParserRegistry


class ManifestParserFactory(BaseFactoryFromRegistry[ETLLayer, BaseManifestParser]):
    @classmethod
    def get_registry(cls):
        """
        Zwraca instancję DataReaderRegistry.
        """
        return ManifestParserRegistry()