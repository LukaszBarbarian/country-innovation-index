# src/common/storage/blob_storage_manager.py

import logging
from typing import Any, Union, Dict, Literal, Optional
from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
import json
from src.common.config.config_manager import ConfigManager
from src.common.models.file_info import FileInfo
from src.common.azure_clients.base_azure_client_manager import AzureClientManagerBase

logger = logging.getLogger(__name__)

class BlobClientManager(AzureClientManagerBase[BlobServiceClient, ContainerClient]):
    """
    Manages connections and operations for an Azure Blob Storage container.

    This class provides methods for uploading, downloading, listing, and deleting blobs,
    and extends the base class for managing Azure clients. It handles the lifecycle
    of the BlobServiceClient and ContainerClient, and supports asynchronous operations.
    """
    
    def __init__(self,
                 container_name: str,
                 storage_account_name_setting_name: str = "DATA_LAKE_STORAGE_ACCOUNT_NAME",
                 config: Optional[ConfigManager] = None):
        """
        Initializes the BlobClientManager.

        Args:
            container_name (str): The name of the blob container.
            storage_account_name_setting_name (str): The name of the environment variable
                                                     storing the storage account name.
        """
        super().__init__(
            resource_name=container_name,
            storage_account_name_setting_name=storage_account_name_setting_name,
            base_url_suffix=".blob.core.windows.net",
            config=config
        )

    def _create_service_client_from_identity(self, account_url: str, credential) -> BlobServiceClient:
        """
        Creates a BlobServiceClient using a credential.
        """
        return BlobServiceClient(account_url=account_url, credential=credential)

    def _get_resource_client(self, service_client: BlobServiceClient, resource_name: str) -> ContainerClient:
        """
        Gets a ContainerClient from the BlobServiceClient.
        """
        return service_client.get_container_client(resource_name)

    async def upload_blob(self,
                          file_content_bytes: Union[Dict[str, Any], str, bytes],
                          file_info: FileInfo,
                          overwrite: bool = True,
                          tags: Optional[Dict[str, str]] = None
                         ) -> int:
        """
        Uploads data to a blob in the container.

        The data can be a dictionary, string, or bytes, and is converted to bytes for upload.
        It also checks for the existence of a file with the same hash name before uploading.

        Args:
            file_content_bytes (Union[Dict[str, Any], str, bytes]): The data to upload.
            file_info (FileInfo): An object containing information about the file.
            overwrite (bool): If True, overwrites the blob if it already exists.
            tags (Optional[Dict[str, str]]): Tags to be associated with the blob (currently not supported).

        Returns:
            int: The size of the uploaded data in bytes.

        Raises:
            TypeError: If the data type is not supported.
            Exception: If the upload fails for any other reason.
        """
        # Convert data to bytes if necessary
        if isinstance(file_content_bytes, dict):
            content_to_upload = json.dumps(file_content_bytes, indent=2).encode('utf-8')
        elif isinstance(file_content_bytes, str):
            content_to_upload = file_content_bytes.encode('utf-8')
        elif isinstance(file_content_bytes, bytes):
            content_to_upload = file_content_bytes
        else:
            logger.error(f"Unsupported data type for upload: {type(file_content_bytes)}. Must be dict, str, or bytes.")
            raise TypeError(f"Unsupported data type for upload: {type(file_content_bytes)}. Must be dict, str, or bytes.")

        # Handle container creation
        try:
            await self.client.create_container()
        except ResourceExistsError:
            logger.debug(f"Container '{self.resource_name}' already exists. Skipping creation.")
        except Exception as container_creation_error:
            logger.error(f"Unexpected error when creating container '{self.resource_name}': {container_creation_error}", exc_info=True)
            raise # Re-raise other unexpected container creation errors

        try:
            folder_path = "/".join(file_info.full_path_in_container.split("/")[:-1]) + "/"
            if await self.blob_with_same_hash_name_exists(folder_path, file_info.hash_name):
                logger.info(f"File with hash_name {file_info.hash_name} already exists in {folder_path}. Skipping upload.")
                return 0

            blob_name = file_info.full_path_in_container
            blob_client = self.client.get_blob_client(blob_name)
            
            # --- CHANGE HERE: Removing 'tags' parameter to avoid "FeatureNotYetSupportedForHierarchicalNamespaceAccounts" error ---
            # If in the future you ABSOLUTELY need tags, you will have to reconsider this.
            # But for now, for "standard file uploads," removing tags is crucial.
            await blob_client.upload_blob(content_to_upload, overwrite=overwrite)
            # --- END OF CHANGE ---
            
            logger.info(f"Data uploaded successfully to {self.resource_name}/{blob_name}")
            
            return len(content_to_upload)
        except Exception as e:
            logger.error(f"Failed to upload file to {file_info.full_path_in_container}: {e}", exc_info=True)
            raise

    async def download_blob(self, blob_path: str, decode_as: Union[None, Literal['text'], Literal['json']] = None) -> Any:
        """
        Downloads data from a blob.

        Args:
            blob_path (str): The path to the blob.
            decode_as (Union[None, Literal['text'], Literal['json']]): The desired decoding format.
                                                                        'text' for UTF-8 string,
                                                                        'json' for a JSON object,
                                                                        None for raw bytes.

        Returns:
            Any: The decoded content or raw bytes of the blob.

        Raises:
            ResourceNotFoundError: If the blob does not exist.
            ValueError: If JSON decoding fails.
            Exception: If the download fails for any other reason.
        """
        try:
            blob_client = self.client.get_blob_client(blob_path)
            download_stream = await blob_client.download_blob()
            data_bytes = await download_stream.readall()
            
            if decode_as == 'json':
                return json.loads(data_bytes)
            elif decode_as == 'text':
                return data_bytes.decode('utf-8')
            return data_bytes
        except ResourceNotFoundError:
            logger.error(f"Blob '{blob_path}' not found in container '{self.resource_name}'.")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error for blob '{blob_path}': {e}")
            raise ValueError(f"Invalid JSON format for blob '{blob_path}'.")
        except Exception as e:
            logger.error(f"Error downloading blob '{blob_path}': {e}", exc_info=True)
            raise

    async def list_blobs(self, name_starts_with: Optional[str] = None) -> list[str]:
        """
        Lists the names of blobs in the container.

        Args:
            name_starts_with (Optional[str]): A prefix to filter the list of blobs.

        Returns:
            list[str]: A list of blob names.

        Raises:
            Exception: If listing blobs fails.
        """
        try:
            blob_list = []
            async for blob in self.client.list_blobs(name_starts_with=name_starts_with):
                blob_list.append(blob.name)
            logger.info(f"Listed {len(blob_list)} blobs in container '{self.resource_name}' with prefix '{name_starts_with or ''}'.")
            return blob_list
        except Exception as e:
            logger.error(f"Error listing blobs in container '{self.resource_name}': {e}", exc_info=True)
            raise

    async def delete_blob(self, blob_name: str):
        """
        Deletes a blob from the container.

        Args:
            blob_name (str): The name of the blob to delete.

        Raises:
            ResourceNotFoundError: If the blob does not exist.
            Exception: If deletion fails for any other reason.
        """
        try:
            await self.client.delete_blob(blob_name)
            logger.info(f"Blob '{blob_name}' deleted from container '{self.resource_name}'.")
        except ResourceNotFoundError:
            logger.warning(f"Attempted to delete non-existent blob '{blob_name}' in container '{self.resource_name}'.")
        except Exception as e:
            logger.error(f"Error deleting blob '{blob_name}': {e}", exc_info=True)
            raise

    # Add asynchronous methods to the context manager
    async def __aenter__(self):
        # self.client (ContainerClient) is already initialized in the base class __init__
        # You could add await self.client.create_container(fail_on_exist=False) here
        # if you want to ensure the container exists before starting anything.
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.close() # Close ContainerClient
        # Additionally, if you want to close BlobServiceClient, you need to store it in the base class
        if hasattr(self, '_service_client') and self._service_client:
            await self._service_client.close()

    async def blob_with_same_hash_name_exists(self, folder_path: str, hash_name: str) -> bool:
        """
        Checks if a blob exists in a folder (prefix) with a name containing the 'hash_name' string.

        Args:
            folder_path (str): The path to the folder, e.g., "folder1/folder2/".
            hash_name (str): The hash to search for in the blob name.

        Returns:
            True if a matching blob exists, False otherwise.
        """
        try:
            # Ensure name_starts_with has the correct format
            prefix = folder_path if folder_path.endswith('/') else folder_path + '/'
            
            # Get blobs with the folder prefix
            blobs = self.client.list_blobs(name_starts_with=prefix)
            
            async for blob in blobs:
                # Condition to check if the given hash is in the blob's name
                if hash_name in blob.name:
                    logger.info(f"Found existing blob '{blob.name}' containing hash: {hash_name}")
                    return True
            
            # If the loop finished and nothing was found
            return False
            
        except Exception as e:
            logger.error(f"An error occurred while checking for blobs with hash '{hash_name}': {e}", exc_info=True)
            raise

    async def get_blob_properties(self, blob_path: str) -> Optional[dict]:
        """
        Gets only the properties (including ETag) of a blob without downloading the content.

        Args:
            blob_path (str): The path to the blob.

        Returns:
            Optional[dict]: A dictionary of blob properties, or None if the blob is not found.
        """
        blob_client = self.client.get_blob_client(blob_path)
        try:
            properties = await blob_client.get_blob_properties()
            return properties
        except ResourceNotFoundError:
            logger.info(f"Blob '{blob_path}' not found.")
            return None
        except Exception as e:
            logger.error(f"Error getting properties for blob '{blob_path}': {e}", exc_info=True)
            raise