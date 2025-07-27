# src/common/storage_path_builder/storage_path_builder_factory.py
from src.common.enums.domain_source import DomainSource
from src.common.storage_metadata_file_builder.base_storage_path_builder import BaseStorageFileMetadataBuilder
from src.common.storage_metadata_file_builder.storage_metadata_file_builder_registry import StorageMetadataFileBuilderRegistry
from src.common.storage_metadata_file_builder.default_storage_metadata_file_builder import DefaultStorageFileMetadataBuilder
from src.common.factory.base_factory import BaseFactoryFromRegistry # Dodaj import
from src.common.registery.base_registry import BaseRegistry # Dodaj import

class StorageMetadataFileBuilderFactory(BaseFactoryFromRegistry[DomainSource, BaseStorageFileMetadataBuilder]):
    @classmethod
    def get_registry(cls) -> BaseRegistry:
        return StorageMetadataFileBuilderRegistry

    @classmethod
    def get_instance(cls, source: DomainSource) -> BaseStorageFileMetadataBuilder:
        try:
            return super().get_instance(source)
        except KeyError:
            return DefaultStorageFileMetadataBuilder()