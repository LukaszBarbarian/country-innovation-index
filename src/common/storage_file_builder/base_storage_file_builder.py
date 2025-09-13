# src/common/storage_file_builder/base_storage_file_builder.py
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Dict, Any, Tuple
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase

class BaseStorageFileBuilder(ABC):
    """
    An abstract base class for building storage file paths and metadata.

    This class provides a common interface for different builders that
    create paths and associated information for files stored in Azure Data Lake.
    """
    def __init__(self, config: ConfigManager):
        """
        Initializes the base storage file builder with a configuration manager.
        """
        self.config = config

    def build_blob_url(self, container_name: str, blob_path: str, account_name: str) -> str:
        """
        Constructs the full URL for a blob in Azure Storage.

        Args:
            container_name (str): The name of the storage container.
            blob_path (str): The path to the blob within the container.
            account_name (str): The name of the storage account.

        Returns:
            str: The complete HTTPS URL to the blob.
        """
        clean_path = blob_path.lstrip("/")
        return f"https://{account_name}.blob.core.windows.net/{container_name.lower()}/{clean_path}"

    @abstractmethod
    def build_file(self,
                   correlation_id: str,
                   container_name: str,
                   storage_account_name: str,
                   **kwargs: Any) -> Dict[str, Any]:
        """
        An abstract method to prepare all data for a file to be written to storage.

        Subclasses must implement this to define the logic for building file paths,
        metadata, and content (if applicable).

        Args:
            correlation_id (str): A unique ID to correlate the process.
            container_name (str): The name of the target container.
            storage_account_name (str): The name of the storage account.
            **kwargs: Additional keyword arguments specific to the builder's needs.

        Returns:
            Dict[str, Any]: A dictionary containing information for writing the file,
                            such as paths and metadata.
        """
        pass

    @abstractmethod
    def build_summary_file_output(self,
                                  context: ContextBase,
                                  results: list,
                                  storage_account_name: str,
                                  container_name: str,
                                  **kwargs: Any) -> Dict[str, Any]:
        """
        An abstract method to build the output for a summary file.

        This method is responsible for generating the content, path, and metadata
        for a summary file based on a list of results from a process.

        Args:
            context (ContextBase): The context of the current process.
            results (list): A list of results to be included in the summary.
            storage_account_name (str): The name of the storage account.
            container_name (str): The name of the target container.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: A dictionary containing information for writing the summary file.
        """
        pass