# src/common/storage_path_builder/storage_path_builder_factory.py
from src.common.enums.etl_layers import ETLLayer
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.common.registers.storage_file_builder_registry import StorageFileBuilderRegistry
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.registers.base_registry import BaseRegistry

class StorageFileBuilderFactory(BaseFactoryFromRegistry[ETLLayer, BaseStorageFileBuilder]):
    """
    A factory for creating instances of storage file builders.

    This class extends `BaseFactoryFromRegistry` and is responsible for
    retrieving and instantiating the correct storage file builder based on the
    ETL layer (e.g., Bronze, Silver, Gold).
    """
    @classmethod
    def get_registry(cls) -> BaseRegistry:
        """
        Returns the registry containing the mapping of ETL layers to storage file builder classes.
        
        This method is a required part of the factory pattern, connecting the factory
        to its specific registry.
        """
        return StorageFileBuilderRegistry()