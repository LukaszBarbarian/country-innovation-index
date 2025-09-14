# src/common/event_grid/event_grid_client_manager.py

import logging
from typing import Dict, Any, Optional

from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.eventgrid import EventGridPublisherClient, EventGridEvent

logger = logging.getLogger(__name__)

class EventGridClientManager:
    """
    Manages the connection and sending of events to an Azure Event Grid topic.
    
    This class can authenticate using either a key or Azure Managed Identity,
    and provides a simple method to publish a single Event Grid event.
    """
    def __init__(self, endpoint: Optional[str] = None, key: Optional[str] = None):
        """
        Initializes the Event Grid client.
        
        It can use a key or Managed Identity (if no key is provided).

        Args:
            endpoint (str): The endpoint URL of the Event Grid topic.
            key (Optional[str]): The access key for the Event Grid topic.
        
        Raises:
            ValueError: If the endpoint is not provided.
        """
        if endpoint and key:
            self.client = EventGridPublisherClient(endpoint, AzureKeyCredential(key))
            logger.info("EventGridPublisherClient initialized with key.")
        elif endpoint:
            self.client = EventGridPublisherClient(endpoint, DefaultAzureCredential())
            logger.info("EventGridPublisherClient initialized with Managed Identity.")
        else:
            raise ValueError("Event Grid endpoint is required.")
            
    def send_event(self, event_type: str, subject: str, data: Dict[str, Any], data_version: str = "1.0") -> Dict[str, Any]:
        """
        Sends a single event to Event Grid.

        Args:
            event_type (str): The type of the event, e.g., 'Contoso.Items.ItemReceived'.
            subject (str): The subject of the event, used for routing, e.g., 'Contoso/Items/Item1'.
            data (Dict[str, Any]): The payload of the event.
            data_version (str): The version of the event data schema.
        
        Returns:
            Dict[str, Any]: A dictionary with the status of the send operation.
        """
        try:
            logger.info(f"Preparing Event Grid event. Type: {event_type}, Subject: {subject}")
            event = EventGridEvent(
                subject=subject,
                event_type=event_type,
                data=data,
                data_version=data_version
            )
            logger.info(f"Sending event to Event Grid: {event}")
            self.client.send([event])
            logger.info("Event sent successfully.")
            return {"status": "EVENT_SENT"}
        except Exception as e:
            logger.exception(f"Error while sending event: {e}")
            return {"status": "FAILED", "message": str(e)}
        

class EventGridNotifier:
    """
    A unified class to handle the logic of sending notifications to Event Grid.

    It encapsulates the configuration and event formatting logic, allowing
    it to be reused across different parts of the application. It simplifies
    the process by building the event subject and type based on provided parameters.
    """
    def __init__(self, endpoint: str, key: Optional[str] = None):
        """
        Initializes the Event Grid Notifier.

        Args:
            endpoint (str): The endpoint URL of the Event Grid topic.
            key (Optional[str]): The access key for the Event Grid topic.
        
        Raises:
            ValueError: If the endpoint is not provided.
        """
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
        
        The method constructs a standardized subject and event type based on the
        provided layer and correlation ID for easy categorization and routing.

        Args:
            layer (str): The ETL layer (e.g., 'bronze', 'silver').
            event_type (str): The specific type of event (e.g., 'IngestionCompleted').
            data (Dict[str, Any]): The payload of the event.
            correlation_id (str): An optional unique ID for tracing.
            
        Returns:
            Dict[str, Any]: The result of the send operation from the underlying client.
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