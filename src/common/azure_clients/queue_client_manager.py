# src/common/storage/queue_storage_manager.py

import logging
from typing import Optional, Union, Dict, Any
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage
from azure.core.exceptions import ResourceNotFoundError

# WAŻNE: Upewnij się, że ta ścieżka importu jest poprawna.
from src.common.azure_clients.blob_client_manager import AzureClientManagerBase


logger = logging.getLogger(__name__)

class QueueStorageManager(AzureClientManagerBase[QueueServiceClient, QueueClient]):
    def __init__(self, 
                 resource_name: str, 
                 storage_account_name_setting_name: str = "QUEUE_STORAGE_ACCOUNT"):
        
        super().__init__(
            resource_name=resource_name,
            storage_account_name_setting_name=storage_account_name_setting_name,
            base_url_suffix=".queue.core.windows.net"
        )

    
    def _create_service_client_from_identity(self, account_url: str, credential) -> QueueServiceClient:
        return QueueServiceClient(account_url=account_url, credential=credential)

    def _get_resource_client(self, service_client: QueueServiceClient, resource_name: str) -> QueueClient:
        return service_client.get_queue_client(resource_name)


    def send_message(self, message_content: Union[str, bytes], encode_base64: bool = True):
        """
        Wysyła wiadomość do kolejki.
        """
        if not self.client:
            raise RuntimeError("QueueClient nie został zainicjowany.")
        try:
            self.client.send_message(message_content)
            logger.info(f"Wiadomość wysłana do kolejki '{self.resource_name}'.")
        except Exception as e:
            logger.error(f"Błąd podczas wysyłania wiadomości do kolejki '{self.resource_name}': {e}")
            raise

    def receive_messages(self, max_messages: int = 1, visibility_timeout: int = 30) -> list[QueueMessage]:
        """
        Odbiera wiadomości z kolejki.
        
        Args:
            max_messages (int): Maksymalna liczba wiadomości do odebrania (1 do 32).
            visibility_timeout (int): Czas (w sekundach), przez który wiadomości będą niewidoczne po odebraniu.
                                     Domyślnie 30 sekund.
        Returns:
            list[QueueMessage]: Lista obiektów QueueMessage.
        """
        if not self.client:
            raise RuntimeError("QueueClient nie został zainicjowany.")
        try:
            messages = self.client.receive_messages(max_messages=max_messages, visibility_timeout=visibility_timeout)
            messages_list = list(messages)
            logger.info(f"Odebrano {len(messages_list)} wiadomości z kolejki '{self.resource_name}'.")
            return messages_list
        except ResourceNotFoundError:
            logger.warning(f"Kolejka '{self.resource_name}' nie istnieje podczas próby odbioru wiadomości. Zwracam pustą listę.")
            return []
        except Exception as e:
            logger.error(f"Błąd podczas odbierania wiadomości z kolejki '{self.resource_name}': {e}")
            raise

    def delete_message(self, message: Union[QueueMessage, Any]):
        """
        Usuwa wiadomość z kolejki.
        
        Args:
            message: Obiekt QueueMessage do usunięcia (odebrany przez receive_messages).
        """
        if not self.client:
            raise RuntimeError("QueueClient nie został zainicjowany.")
        try:
            self.client.delete_message(message)
            logger.info(f"Wiadomość usunięta z kolejki '{self.resource_name}'.")
        except Exception as e:
            logger.error(f"Błąd podczas usuwania wiadomości z kolejki '{self.resource_name}': {e}")
            raise
            
    def peek_messages(self, max_messages: int = 1) -> list[Any]:
        """
        Podgląda wiadomości w kolejce bez usuwania ich ani ustawiania niewidoczności.
        
        Args:
            max_messages (int): Maksymalna liczba wiadomości do podglądu (1 do 32).
        Returns:
            list[PeekedMessage]: Lista obiektów PeekedMessage.
        """
        if not self.client:
            raise RuntimeError("QueueClient nie został zainicjowany.")
        try:
            messages = self.client.peek_messages(max_messages=max_messages)
            logger.info(f"Podglądano {len(messages)} wiadomości z kolejki '{self.resource_name}'.")
            return list(messages)
        except ResourceNotFoundError:
            logger.warning(f"Kolejka '{self.resource_name}' nie istnieje podczas próby podglądu wiadomości. Zwracam pustą listę.")
            return []
        except Exception as e:
            logger.error(f"Błąd podczas podglądania wiadomości z kolejki '{self.resource_name}': {e}")
            raise

    def get_queue_length(self) -> int:
        """
        Zwraca szacowaną liczbę wiadomości w kolejce.
        """
        if not self.client:
            raise RuntimeError("QueueClient nie został zainicjowany.")
        try:
            properties = self.client.get_queue_properties()
            length = properties.approximate_message_count

            if length is None:
                length = 0
                
            logger.info(f"Kolejka '{self.resource_name}' ma około {length} wiadomości.")
            return length
        except ResourceNotFoundError:
            logger.warning(f"Kolejka '{self.resource_name}' nie istnieje, zwrócono długość 0.")
            return 0
        except Exception as e:
            logger.error(f"Błąd podczas pobierania długości kolejki '{self.resource_name}': {e}")
            raise

    def create_queue_if_not_exists(self):
        """
        Tworzy kolejkę, jeśli jeszcze nie istnieje.
        """
        if not self.client:
            raise RuntimeError("QueueClient nie został zainicjowany.")
        try:
            self.client.create_queue()
            logger.info(f"Kolejka '{self.resource_name}' została utworzona (lub już istniała).")
        except Exception as e:
            logger.error(f"Błąd podczas tworzenia kolejki '{self.resource_name}': {e}")
            raise

    def delete_queue_if_exists(self):
        """
        Usuwa kolejkę, jeśli istnieje.
        """
        if not self.client:
            raise RuntimeError("QueueClient nie został zainicjowany.")
        try:
            self.client.delete_queue()
            logger.info(f"Kolejka '{self.resource_name}' została usunięta (lub nie istniała).")
        except Exception as e:
            logger.error(f"Błąd podczas usuwania kolejki '{self.resource_name}': {e}")
            raise