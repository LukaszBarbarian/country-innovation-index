# src/common/storage/queue_storage_manager.py

import logging
from typing import Optional, Union, Dict, Any
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage
from azure.core.exceptions import ResourceNotFoundError

# IMPORTANT: Ensure this import path is correct.
from src.common.azure_clients.blob_client_manager import AzureClientManagerBase


logger = logging.getLogger(__name__)

class QueueStorageManager(AzureClientManagerBase[QueueServiceClient, QueueClient]):
    """
    This class manages operations on Azure Storage Queues. It inherits from the base class
    AzureClientManagerBase, which allows for the standardization of authentication and client initialization.
    """
    def __init__(self, 
                 resource_name: str, 
                 storage_account_name_setting_name: str = "QUEUE_STORAGE_ACCOUNT"):
        """
        Initializes the queue manager.
        
        Args:
            resource_name (str): The name of the queue.
            storage_account_name_setting_name (str): The name of the setting in the configuration file
                                                      that contains the storage account name.
        """
        super().__init__(
            resource_name=resource_name,
            storage_account_name_setting_name=storage_account_name_setting_name,
            base_url_suffix=".queue.core.windows.net"
        )

    
    def _create_service_client_from_identity(self, account_url: str, credential) -> QueueServiceClient:
        """
        A factory method to create a QueueServiceClient using Azure Identity
        (e.g., DefaultAzureCredential).
        """
        return QueueServiceClient(account_url=account_url, credential=credential)

    def _get_resource_client(self, service_client: QueueServiceClient, resource_name: str) -> QueueClient:
        """
        A factory method to create a client for a specific queue.
        """
        return service_client.get_queue_client(resource_name)


    def send_message(self, message_content: Union[str, bytes], encode_base64: bool = True):
        """
        Sends a message to the queue. The message can be a string or bytes.
        
        Args:
            message_content (Union[str, bytes]): The content of the message.
            encode_base64 (bool): Whether the message should be Base64 encoded (True by default).
        """
        if not self.client:
            raise RuntimeError("QueueClient has not been initialized.")
        try:
            self.client.send_message(message_content)
            logger.info(f"Message sent to queue '{self.resource_name}'.")
        except Exception as e:
            logger.error(f"Error sending message to queue '{self.resource_name}': {e}")
            raise

    def receive_messages(self, max_messages: int = 1, visibility_timeout: int = 30) -> list[QueueMessage]:
        """
        Receives messages from the queue and makes them invisible for a specified time.
        
        Args:
            max_messages (int): The maximum number of messages to receive (1 to 32).
            visibility_timeout (int): The time (in seconds) during which messages will be invisible
                                     after being received. Defaults to 30 seconds.
        Returns:
            list[QueueMessage]: A list of QueueMessage objects.
        """
        if not self.client:
            raise RuntimeError("QueueClient has not been initialized.")
        try:
            messages = self.client.receive_messages(max_messages=max_messages, visibility_timeout=visibility_timeout)
            messages_list = list(messages)
            logger.info(f"Received {len(messages_list)} messages from queue '{self.resource_name}'.")
            return messages_list
        except ResourceNotFoundError:
            logger.warning(f"Queue '{self.resource_name}' does not exist when attempting to receive messages. Returning an empty list.")
            return []
        except Exception as e:
            logger.error(f"Error receiving messages from queue '{self.resource_name}': {e}")
            raise

    def delete_message(self, message: Union[QueueMessage, Any]):
        """
        Deletes a message from the queue. It is important to use the message object
        received by the receive_messages method.
        
        Args:
            message: The QueueMessage object to delete.
        """
        if not self.client:
            raise RuntimeError("QueueClient has not been initialized.")
        try:
            self.client.delete_message(message)
            logger.info(f"Message deleted from queue '{self.resource_name}'.")
        except Exception as e:
            logger.error(f"Error deleting message from queue '{self.resource_name}': {e}")
            raise
            
    def peek_messages(self, max_messages: int = 1) -> list[Any]:
        """
        Peeks at messages in the queue without deleting them or changing their visibility.
        
        Args:
            max_messages (int): The maximum number of messages to peek at (1 to 32).
        Returns:
            list[PeekedMessage]: A list of PeekedMessage objects.
        """
        if not self.client:
            raise RuntimeError("QueueClient has not been initialized.")
        try:
            messages = self.client.peek_messages(max_messages=max_messages)
            logger.info(f"Peeked at {len(messages)} messages from queue '{self.resource_name}'.")
            return list(messages)
        except ResourceNotFoundError:
            logger.warning(f"Queue '{self.resource_name}' does not exist when attempting to peek at messages. Returning an empty list.")
            return []
        except Exception as e:
            logger.error(f"Error peeking at messages from queue '{self.resource_name}': {e}")
            raise

    def get_queue_length(self) -> int:
        """
        Returns the approximate number of messages in the queue.
        
        Returns:
            int: The approximate number of messages. Returns 0 if the queue does not exist.
        """
        if not self.client:
            raise RuntimeError("QueueClient has not been initialized.")
        try:
            properties = self.client.get_queue_properties()
            length = properties.approximate_message_count

            if length is None:
                length = 0
                
            logger.info(f"Queue '{self.resource_name}' has approximately {length} messages.")
            return length
        except ResourceNotFoundError:
            logger.warning(f"Queue '{self.resource_name}' does not exist, returned a length of 0.")
            return 0
        except Exception as e:
            logger.error(f"Error getting queue length for '{self.resource_name}': {e}")
            raise

    def create_queue_if_not_exists(self):
        """
        Creates the queue if it does not already exist. This operation is idempotent.
        """
        if not self.client:
            raise RuntimeError("QueueClient has not been initialized.")
        try:
            self.client.create_queue()
            logger.info(f"Queue '{self.resource_name}' was created (or already existed).")
        except Exception as e:
            logger.error(f"Error creating queue '{self.resource_name}': {e}")
            raise

    def delete_queue_if_exists(self):
        """
        Deletes the queue if it exists. This operation is idempotent.
        """
        if not self.client:
            raise RuntimeError("QueueClient has not been initialized.")
        try:
            self.client.delete_queue()
            logger.info(f"Queue '{self.resource_name}' was deleted (or did not exist).")
        except Exception as e:
            logger.error(f"Error deleting queue '{self.resource_name}': {e}")
            raise