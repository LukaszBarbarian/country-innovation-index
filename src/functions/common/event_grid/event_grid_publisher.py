import os
import requests
import json
import logging
import uuid
from datetime import datetime
from azure.identity import DefaultAzureCredential

class EventGridPublisher:
    """
    Klasa do publikowania niestandardowych zdarzeń do Azure Event Grid.
    Wykorzystuje DefaultAzureCredential do automatycznego uwierzytelniania
    za pomocą Managed Identity na Azure lub lokalnych poświadczeń (np. Azure CLI).
    """

    def __init__(self, topic_endpoint: str):
        """
        Inicjalizuje wydawcę Event Grid.

        Args:
            topic_endpoint (str): Adres URL endpointu Event Grid Topic.
                                  Np. 'https://your-topic.northeurope-1.eventgrid.azure.net/api/events'
        """
        if not topic_endpoint:
            raise ValueError("Event Grid Topic Endpoint cannot be empty.")
        self.topic_endpoint = topic_endpoint
        self._credential: DefaultAzureCredential  = DefaultAzureCredential() # Inicjalizuj poza metodami
        self._logger = logging.getLogger(__name__)

    def _get_access_token(self) -> str:
        """
        Pobiera token dostępu dla Event Grid używając DefaultAzureCredential.
        """
        try:
            # Zakres dla Event Grid.
            # Upewnij się, że Managed Identity Function App ma rolę "EventGrid Data Sender"
            # na Event Grid Topic.
            token_response = self._credential.get_token("https://eventgrid.azure.net/.default")
            return token_response.token
        except Exception as e:
            self._logger.error(f"Failed to get access token for Event Grid: {e}")
            raise # Ponownie rzuć wyjątek, aby wywołujący kod mógł go obsłużyć

    def publish_file_upload_event(
        self,
        file_name: str,
        file_path: str,
        file_size: int,
        storage_account_name: str,
        container_name: str,
        correlation_id: str = None # Opcjonalny ID do śledzenia
    ) -> bool:
        """
        Publikuje niestandardowe zdarzenie "Storage.BlobCreated.Custom" do Event Grid.

        Args:
            file_name (str): Nazwa zapisanego pliku.
            file_path (str): Pełna ścieżka do pliku w kontenerze.
            file_size (int): Rozmiar pliku w bajtach.
            storage_account_name (str): Nazwa konta magazynowego.
            container_name (str): Nazwa kontenera.
            correlation_id (str, optional): Opcjonalny ID korelacji. Domyślnie None.

        Returns:
            bool: True, jeśli zdarzenie zostało pomyślnie opublikowane, False w przeciwnym razie.
        """
        try:
            access_token = self._get_access_token()

            event_id = str(uuid.uuid4())
            event_time = datetime.utcnow().isoformat() + "Z"

            # Konstruowanie URL do pliku w storage
            file_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{file_path}"

            # Tworzenie zdarzenia w formacie CloudEvents 1.0
            # https://cloudevents.io/spec/
            event_data = {
                "id": event_id,
                "source": f"/subscriptions/{os.environ.get('AZURE_SUBSCRIPTION_ID', 'unknown')}/resourceGroups/{os.environ.get('WEBSITE_RESOURCE_GROUP', 'unknown')}/providers/Microsoft.Web/sites/{os.environ.get('WEBSITE_SITE_NAME', 'unknown')}",
                "subject": f"/blobServices/default/containers/{container_name}/blobs/{file_path}",
                "type": "Storage.BlobCreated.Custom", # Niestandardowy typ zdarzenia
                "time": event_time,
                "datacontenttype": "application/json",
                "data": {
                    "api": "PutBlob", # Możesz dostosować, jeśli operacja jest inna
                    "url": file_url,
                    "contentType": "application/octet-stream", # Dostosuj, jeśli znasz typ MIME
                    "contentLength": file_size,
                    "blobType": "BlockBlob",
                    "fileName": file_name,
                    "correlationId": correlation_id # Dodaj opcjonalny correlation_id
                },
                "specversion": "1.0"
            }

            headers = {
                "Content-Type": "application/cloudevents+json", # Ważne dla CloudEvents
                "Authorization": f"Bearer {access_token}"
            }

            # Event Grid oczekuje tablicy zdarzeń, nawet jeśli jest tylko jedno
            response = requests.post(self.topic_endpoint, data=json.dumps([event_data]), headers=headers)

            if response.status_code == 200:
                self._logger.info(f"Event published successfully. Event ID: {event_id}")
                return True
            else:
                self._logger.error(
                    f"Failed to publish event. Status: {response.status_code}, "
                    f"Response: {response.text}, Event ID: {event_id}"
                )
                return False

        except Exception as e:
            self._logger.error(f"An error occurred while publishing event: {e}")
            return False