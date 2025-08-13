# src/common/storage_path_builder/storage_path_builder_registry.py
from src.common.enums.etl_layers import ETLLayer
from src.common.registers.base_registry import BaseRegistry
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from typing import Dict, Type

class StorageFileBuilderRegistry(BaseRegistry[ETLLayer, BaseStorageFileBuilder]):
    _registry: Dict[ETLLayer, Type[BaseStorageFileBuilder]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[ETLLayer, Type[BaseStorageFileBuilder]]:
        return cls._registry