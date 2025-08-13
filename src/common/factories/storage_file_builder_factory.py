# src/common/storage_path_builder/storage_path_builder_factory.py
from src.common.enums.domain_source import DomainSource
from src.common.storage_file_builder.base_storage_file_builder import BaseStorageFileBuilder
from src.common.registers.storage_file_builder_registry import StorageFileBuilderRegistry
from src.common.factories.base_factory import BaseFactoryFromRegistry
from src.common.registers.base_registry import BaseRegistry

class StorageFileBuilderFactory(BaseFactoryFromRegistry[DomainSource, BaseStorageFileBuilder]):
    @classmethod
    def get_registry(cls) -> BaseRegistry:
        return StorageFileBuilderRegistry()