# src/common/storage_path_builder/storage_path_builder_registry.py
from src.common.registery.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.storage_metadata_file_builder.base_storage_path_builder import BaseStorageFileMetadataBuilder

class StorageMetadataFileBuilderRegistry(BaseRegistry[DomainSource, BaseStorageFileMetadataBuilder]):
    pass