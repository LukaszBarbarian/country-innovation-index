# shared_code/decorators.py

from __future__ import annotations

import azure.functions as func
from functools import wraps
from typing import Callable, Awaitable
from bronze_ingestion.api_factory.types import ApiType
from bronze_ingestion.api_factory.factory import ApiFactory
from bronze_ingestion.api_client.base import ApiClient, ApiResponse
from shared.storage_account.bronze_storage_manager import BronzeStorageManager

def ingest_data_pipeline(api_type: ApiType):
    def decorator(func_to_decorate: Callable[[func.HttpRequest], Awaitable[tuple[dict, str] | func.HttpResponse]]): # Zmieniono typ zwracany przez dekorowaną funkcję
        @wraps(func_to_decorate)
        async def wrapper(req: func.HttpRequest) -> func.HttpResponse:
            print(f"[INFO] Function '{func_to_decorate.__name__}' triggered for API: {api_type.value}")

            try:
                result = await func_to_decorate(req)

                # Jeśli dekorowana funkcja zwróciła HttpResponse, zwróć go od razu
                if isinstance(result, func.HttpResponse):
                    print(f"[DEBUG] Decoratee returned HttpResponse early: {result.status_code}")
                    return result
                
                # W przeciwnym razie, rozpakuj to, co oczekiwano
                api_params, dataset_name_for_storage = result 
                print(f"[DEBUG] API params: {api_params}, dataset name: {dataset_name_for_storage}")

                api_client: ApiClient = ApiFactory.get_api_client(api_type)

                response: ApiResponse = api_client.fetch_data(**api_params)

                if not response.ok:
                    return func.HttpResponse(f"[ERROR] API returned status {response.status_code}", status_code=502)

                data_to_save = response.json()
                print(f"[DEBUG] Retrieved data type: {type(data_to_save)}")

                api_name = api_type.value.lower()
                storage_manager = BronzeStorageManager()
                blob_path = storage_manager.upload_json_data(data_to_save, api_name, dataset_name_for_storage)

                return func.HttpResponse(
                    f"[SUCCESS] Ingested data for '{dataset_name_for_storage}' from {api_type.value}. Saved to {blob_path}",
                    status_code=200
                )

            except ValueError as ve:
                print(f"[ERROR] ValueError: {ve}")
                return func.HttpResponse(f"[ERROR] Invalid input or config: {ve}", status_code=400)
            except Exception as e:
                print(f"[ERROR] Unexpected exception: {e}")
                return func.HttpResponse(f"[ERROR] Unexpected exception: {e}", status_code=500)

        return wrapper
    return decorator