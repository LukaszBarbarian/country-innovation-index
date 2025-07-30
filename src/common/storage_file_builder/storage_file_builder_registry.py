# src/common/storage_path_builder/storage_path_builder_registry.py
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from typing import Dict, Type

class StorageFileBuilderRegistry(BaseRegistry[DomainSource, BaseStorageFileBuilder]):
    _registry: Dict[DomainSource, Type[BaseStorageFileBuilder]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[BaseStorageFileBuilder]]:
        return cls._registry