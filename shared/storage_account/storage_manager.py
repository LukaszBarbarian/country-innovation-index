# shared_code/storage_manager_base.py
from __future__ import annotations
import os
import json
from azure.storage.blob import BlobServiceClient, ContentSettings, ContainerClient
from abc import ABC, abstractmethod # Użyjemy ABC dla abstrakcyjności

class StorageManagerBase(ABC):
    def __init__(self, container_name: str):
        from azure.identity import DefaultAzureCredential
        credential = DefaultAzureCredential()

        storage_account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
        if not storage_account_name:
            connection_string = os.environ.get("AzureWebJobsStorage")
            if not connection_string:
                raise ValueError("Brak AZURE_STORAGE_ACCOUNT_NAME lub AzureWebJobsStorage w zmiennych środowiskowych.")
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        else:
            self.blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)

        self.container_name = container_name
        self._container_client = None

    @property
    def container_client(self) -> ContainerClient:
        if self._container_client is None:
            self._container_client = self.blob_service_client.get_container_client(self.container_name)

            try:
                self._container_client.create_container()
                print(f"Container '{self.container_name}' created.")
            except Exception as e:
                if "ContainerAlreadyExists" not in str(e):
                    raise
        return self._container_client

    @abstractmethod
    def upload_data(self, data: dict, path: str) -> str:
        """
        Abstrakcyjna metoda do wgrywania danych.
        Konkretne implementacje będą różnić się w zależności od warstwy.
        """
        pass