# src/common/event_grid/event_grid_client_manager.py

import logging
from typing import Dict, Any, Optional

from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.eventgrid import EventGridPublisherClient, EventGridEvent

logger = logging.getLogger(__name__)

class EventGridClientManager:
    def __init__(self, endpoint: Optional[str] = None, key: Optional[str] = None):
        """
        Inicjalizuje klienta Event Grid. 
        Może używać klucza lub Managed Identity (jeśli nie podano klucza).
        """
        if endpoint and key:
            self.client = EventGridPublisherClient(endpoint, AzureKeyCredential(key))
            logger.info("EventGridPublisherClient initialized with key.")
        elif endpoint:
            self.client = EventGridPublisherClient(endpoint, DefaultAzureCredential())
            logger.info("EventGridPublisherClient initialized with Managed Identity.")
        else:
            raise ValueError("Endpoint dla Event Grid jest wymagany.")
            
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
        



class EventGridNotifier:
    """
    A unified class to handle the logic of sending events to Event Grid.
    It encapsulates the configuration and event formatting logic, allowing
    it to be reused across different parts of the application.
    """
    def __init__(self, endpoint: str, key: Optional[str] = None):
        if not endpoint:
            raise ValueError("Event Grid endpoint is required.")
        self.client = EventGridClientManager(endpoint=endpoint, key=key)

    def send_notification(self, 
                          layer: str,
                          event_type: str,
                          data: Dict[str, Any],
                          correlation_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Sends a generic, layered notification to Event Grid.
        
        Args:
            layer (str): The ETL layer (e.g., 'bronze', 'silver').
            event_type (str): The specific type of event (e.g., 'IngestionCompleted').
            data (Dict[str, Any]): The payload of the event.
            correlation_id (str): An optional unique ID for tracing.
            
        Returns:
            Dict[str, Any]: The result of the send operation.
        """
        if not correlation_id:
            logger.warning("No correlation ID provided for event.")
            subject = f"/{layer}/processing/no-correlation-id"
        else:
            subject = f"/{layer}/processing/{correlation_id}"

        full_event_type = f"{layer.capitalize()}{event_type}"
        
        return self.client.send_event(
            event_type=full_event_type,
            subject=subject,
            data=data
        )
