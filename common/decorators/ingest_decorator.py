# shared_code/decorators.py

from __future__ import annotations

import azure.functions as func
from functools import wraps
import json # <--- DODAJ TEN IMPORT
from typing import Callable, Awaitable
from common.enums.api_type import ApiType
from bronze_ingestion.api_factory.base import ApiFactory
from bronze_ingestion.api_client.base import ApiClient, ApiResponse
from common.storage_account.bronze_storage_manager import BronzeStorageManager

def ingest_data_pipeline(api_type: ApiType):
    def decorator(func_to_decorate: Callable[[func.HttpRequest], Awaitable[tuple[dict, str] | func.HttpResponse]]):
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
                    # Zwracamy JSON z błędem
                    error_payload = {
                        "status": "ERROR",
                        "message": f"API returned status {response.status_code} for {api_type.value}.",
                        "details": response.text() # Dodaj szczegóły z API, jeśli są
                    }
                    return func.HttpResponse(json.dumps(error_payload), mimetype="application/json", status_code=502)

                data_to_save = response.json()
                print(f"[DEBUG] Retrieved data type: {type(data_to_save)}")

                api_name = api_type.value.lower()
                storage_manager = BronzeStorageManager()
                blob_path = storage_manager.upload_json_data(data_to_save, api_name, dataset_name_for_storage)

                # --- ZMIANA TUTAJ: ZWRACAJ JSON ---
                success_payload = {
                    "status": "SUCCESS",
                    "message": f"Ingested data for '{dataset_name_for_storage}' from {api_type.value}. Saved to {blob_path}",
                    "dataset_name": dataset_name_for_storage,
                    "blob_path": blob_path # Dodaj ścieżkę do bloba do odpowiedzi
                }
                return func.HttpResponse(
                    json.dumps(success_payload), # <--- Konwertujemy słownik na string JSON
                    mimetype="application/json", # <--- Ustawiamy Content-Type na JSON
                    status_code=200
                )

            except ValueError as ve:
                print(f"[ERROR] ValueError: {ve}")
                error_payload = {
                    "status": "ERROR",
                    "message": f"Invalid input or config: {ve}"
                }
                return func.HttpResponse(json.dumps(error_payload), mimetype="application/json", status_code=400)
            except Exception as e:
                print(f"[ERROR] Unexpected exception: {e}")
                error_payload = {
                    "status": "ERROR",
                    "message": f"Unexpected server error: {e}"
                }
                return func.HttpResponse(json.dumps(error_payload), mimetype="application/json", status_code=500)

        return wrapper
    return decorator