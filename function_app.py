import uuid
import json
import os
import traceback
import logging
from typing import Dict, Any

import azure.functions as func
import azure.durable_functions as df

# Importy z Twoich modułów
from src.bronze.contexts.bronze_parser import BronzePayloadParser
from src.common.azure_clients.event_grid_client_manager import EventGridClientManager
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer

from src.bronze.init import bronze_init

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)



@app.function_name(name="start_ingestion_http")
@app.route(route="start_ingestion")
@app.durable_client_input(client_name="starter")
async def start_ingestion_http(req: func.HttpRequest, starter: df.DurableOrchestrationClient) -> func.HttpResponse:
    logging.info('start_ingestion_http function triggered.')

    try:
        manifest_payload = req.get_json()
    except ValueError:
        logging.error("Żądanie nie zawiera prawidłowego JSON.")
        return func.HttpResponse("Błąd: Oczekiwano prawidłowego formatu JSON.", status_code=400)
    except Exception as e:
        logging.exception(f"Inny błąd podczas przetwarzania żądania: {e}")
        return func.HttpResponse(f"Błąd: {str(e)}", status_code=500)
    
    try:
        instance_id = await starter.start_new("ingest_orchestrator", None, manifest_payload)
        logging.info(f"Rozpoczęto orkiestrację z ID = '{instance_id}'.")
        return starter.create_check_status_response(req, instance_id)
    except Exception as e:
        logging.exception(f"Błąd podczas uruchamiania orkiestracji: {e}")
        return func.HttpResponse(f"Błąd podczas uruchamiania orkiestracji: {e}", status_code=500)
