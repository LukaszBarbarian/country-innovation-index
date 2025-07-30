# src/functions/common/event_grid/event_grid_publisher.py

import os
import json
import logging
import uuid
from datetime import datetime
from azure.identity.aio import DefaultAzureCredential
import aiohttp

from src.common.models.file_info import FileInfo

logger = logging.getLogger(__name__)

class EventGridClient:
    def __init__(self, topic_endpoint: str, topic_resource_id: str):
        if not topic_endpoint:
            raise ValueError("Event Grid Topic Endpoint cannot be empty.")
        if not topic_resource_id:
            raise ValueError("Event Grid Topic Resource ID cannot be empty.")

        self.topic_endpoint = topic_endpoint
        self.topic_resource_id = topic_resource_id
        self._credential = DefaultAzureCredential()
        self._logger = logging.getLogger(__name__)
        self._session = None

    async def _get_access_token(self) -> str:
        try:
            token_response = await self._credential.get_token("https://eventgrid.azure.net/.default")
            return token_response.token
        except Exception as e:
            self._logger.error(f"Failed to get access token for Event Grid: {e}", exc_info=True)
            raise

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    def _serialize_event_data(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat(timespec='milliseconds') + "Z"
        elif isinstance(obj, dict):
            return {k: self._serialize_event_data(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_event_data(elem) for elem in obj]
        return obj

    async def publish_event(
        self,
        file_info: FileInfo, 
        correlation_id: str = None 
    ) -> bool:
        try:
            access_token = await self._get_access_token()
            session = await self._get_session()

            event_id = str(uuid.uuid4())
            event_time = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"

            event_data_payload = {
                "api": "PutBlob", 
                "url": file_info.url,
                "contentType": "application/json", 
                "contentLength": file_info.file_size_bytes,
                "blobType": "BlockBlob",
                "fileName": file_info.file_name,
                "containerName": file_info.container_name, 
                "storageAccountName": file_info.storage_account_name, 
                "domainSource": file_info.domain_source,
                "datasetName": file_info.dataset_name, 
                "ingestionDate": file_info.ingestion_date, 
                "correlationId": correlation_id 
            }

            serialized_event_data_payload = self._serialize_event_data(event_data_payload)

            event = {
                "id": event_id,
                "topic": self.topic_resource_id, # <--- TUTAJ KLUCZOWA ZMIANA: Używamy pełnego Resource ID
                "subject": f"/datasets/{file_info.dataset_name}/files/{file_info.file_name}",
                "eventType": "Ingestion.FileUploaded",
                "eventTime": event_time,
                "data": serialized_event_data_payload,
                "dataVersion": "1.0"
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }

            async with session.post(self.topic_endpoint, data=json.dumps([event]), headers=headers) as response:
                if response.status == 200:
                    self._logger.info(f"Event published successfully. Event ID: {event_id}, Type: Ingestion.FileUploaded")
                    return True
                else:
                    response_text = await response.text()
                    self._logger.error(
                        f"Failed to publish Ingestion.FileUploaded event. Status: {response.status}, "
                        f"Response: {response_text}, Event ID: {event_id}"
                    )
                    return False

        except Exception as e:
            self._logger.error(f"An error occurred while publishing Ingestion.FileUploaded event: {e}", exc_info=True)
            return False

    async def publish_event_failure(
        self,
        dataset_name: str,
        domain_source: str, 
        correlation_id: str,
        error_message: str
    ) -> bool:
        try:
            access_token = await self._get_access_token()
            session = await self._get_session()

            event_id = str(uuid.uuid4())
            event_time = datetime.utcnow().isoformat(timespec='milliseconds') + "Z"

            event_data_payload = {
                "datasetName": dataset_name,
                "domainSource": domain_source,
                "correlationId": correlation_id,
                "status": "Failed",
                "errorMessage": error_message,
                "failureTime": event_time
            }

            serialized_event_data_payload = self._serialize_event_data(event_data_payload)

            event = {
                "id": event_id,
                "topic": self.topic_resource_id, # <--- TUTAJ KLUCZOWA ZMIANA: Używamy pełnego Resource ID
                "subject": f"/datasets/{dataset_name}/ingestionFailure",
                "eventType": "Ingestion.Failed",
                "eventTime": event_time,
                "data": serialized_event_data_payload,
                "dataVersion": "1.0"
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }

            async with session.post(self.topic_endpoint, data=json.dumps([event]), headers=headers) as response:
                if response.status == 200:
                    self._logger.info(f"Event published successfully. Event ID: {event_id}, Type: Ingestion.Failed")
                    return True
                else:
                    response_text = await response.text()
                    self._logger.error(
                        f"Failed to publish Ingestion.Failed event. Status: {response.status}, "
                        f"Response: {response_text}, Event ID: {event_id}"
                    )
                    return False

        except Exception as e:
            self._logger.error(f"An error occurred while publishing Ingestion.Failed event: {e}", exc_info=True)
            return False

    async def __aenter__(self):
        await self._get_session() 
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._credential:
            await self._credential.close()