# src/common/event_grid/event_grid_client_manager.py

import logging
from typing import Dict, Any, Optional

from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.eventgrid import EventGridPublisherClient, EventGridEvent

from src.common.azure_clients.base_azure_client_manager import AzureClientManagerBase
from src.common.config.config_manager import ConfigManager

logger = logging.getLogger(__name__)

class EventGridClientManager(AzureClientManagerBase[EventGridPublisherClient, EventGridPublisherClient]):
    """
    Menedżer klienta Event Grid, oparty o AzureClientManagerBase.
    Obsługuje autoryzację przy użyciu klucza lub Managed Identity.
    """

    def __init__(self):
        super().__init__(
            resource_name="",  # Event Grid topic nie ma "zasobu" w sensie kontenera/kolejki
            storage_account_name_setting_name="EVENT_GRID_TOPIC_HOSTNAME",
            base_url_suffix=""  # Pełny endpoint dostarczamy z configu
        )

    def _create_service_client_from_identity(self, account_url: str, credential) -> EventGridPublisherClient:
        logger.info(f"Tworzenie EventGridPublisherClient z Managed Identity. URL: {account_url}")
        return EventGridPublisherClient(account_url, credential)

    def _get_resource_client(self, service_client: EventGridPublisherClient, resource_name: str) -> EventGridPublisherClient:
        # W przypadku Event Grid, client i service_client to to samo
        return service_client

    def initialize_from_key(self, endpoint: str, key: str):
        """
        Inicjalizuje klienta Event Grid przy użyciu klucza (KeyCredential).
        """
        logger.info(f"Inicjalizacja EventGridPublisherClient przy użyciu klucza. Endpoint: {endpoint}")
        credential = AzureKeyCredential(key)
        self._service_client = EventGridPublisherClient(endpoint, credential)
        self._client = self._service_client

    def send_event(self, event_type: str, subject: str, data: Dict[str, Any], data_version: str = "1.0") -> Dict[str, Any]:
        """
        Wysyła pojedynczy event do Event Grid.
        """
        try:
            logger.info(f"Przygotowanie zdarzenia Event Grid. Typ: {event_type}, Subject: {subject}")
            event = EventGridEvent(
                subject=subject,
                event_type=event_type,
                data=data,
                data_version=data_version
            )
            logger.info(f"Wysyłanie eventu do Event Grid: {event}")
            self.client.send([event])
            logger.info("Event wysłany pomyślnie.")
            return {"status": "EVENT_SENT"}
        except Exception as e:
            logger.exception(f"Błąd podczas wysyłania eventu: {e}")
            return {"status": "FAILED", "message": str(e)}
