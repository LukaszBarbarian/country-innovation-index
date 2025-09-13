# src/common/storage_path_builder/storage_path_builder_registry.py
from src.common.enums.etl_layers import ETLLayer
from src.common.registers.base_registry import BaseRegistry
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from typing import Dict, Type

class StorageFileBuilderRegistry(BaseRegistry[ETLLayer, BaseStorageFileBuilder]):
    """
    A registry for mapping ETL layers to their corresponding storage file builder classes.
    This class inherits from `BaseRegistry` to provide a consistent mechanism for
    managing and retrieving registered builders.
    """
    _registry: Dict[ETLLayer, Type[BaseStorageFileBuilder]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[ETLLayer, Type[BaseStorageFileBuilder]]:
        """
        Returns the internal dictionary that stores the registered storage file builder classes.
        This method is required by the base class to provide access to the registry data.
        """
        return cls._registry