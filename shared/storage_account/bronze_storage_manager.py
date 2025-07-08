# shared_code/bronze_storage_manager.py
from __future__ import annotations
import json
from datetime import datetime
from .storage_manager import StorageManagerBase

class BronzeStorageManager(StorageManagerBase):
    def __init__(self):
        super().__init__("bronze")

    def upload_json_data(self, data: dict, api_name: str, dataset_name: str) -> str:
        """
        Wgrywa surowe dane JSON do warstwy Bronze.
        Ścieżka: bronze/<api_name>/<rok>/<miesiac>/<dzien>/<timestamp>_<dataset_name>.json
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        year = datetime.utcnow().strftime("%Y")
        month = datetime.utcnow().strftime("%m")
        day = datetime.utcnow().strftime("%d")

        blob_name = f"{api_name}/{year}/{month}/{day}/{timestamp}_{dataset_name}.json"
        
        json_data = json.dumps(data, indent=2)
        
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_data, overwrite=True)
        
        print(f"Uploaded {api_name}/{dataset_name} to {self.container_name}/{blob_name}")
        return f"{self.container_name}/{blob_name}"

    def upload_data(self, data: dict, path: str) -> str:
        raise NotImplementedError("Use upload_json_data for Bronze layer.")

    def get_blob_client_for_path(self, path: str):
        return self.container_client.get_blob_client(path)

    def download_json_data(self, blob_path: str) -> dict:
        blob_client = self.container_client.get_blob_client(blob_path)
        download_stream = blob_client.download_blob()
        return json.loads(download_stream.readall())