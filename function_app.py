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

# ---------------------------------------------------------------------
# 2) Orchestrator
# ---------------------------------------------------------------------
@app.orchestration_trigger(context_name="context")
def ingest_orchestrator(context: df.DurableOrchestrationContext):
    logger.info("ingest_orchestrator started.")
    input_payload = context.get_input()

    if not isinstance(input_payload, dict) or not input_payload:
        logger.error("Orchestrator received an empty or invalid payload.")
        return {"status": "FAILED", "message": "Orchestrator received an empty or invalid payload."}
    
    orchestrator_result_dict = yield context.call_activity(
        "run_bronze_orchestrator_activity", 
        {"input_payload": input_payload}
    )

    logger.info(f"Completed Bronze orchestration. Result: {orchestrator_result_dict}")

    silver_manifest_path = "/silver/configs/dev.config.json"
    
    event_grid_payload = {
        "layer": "bronze",
        "env": input_payload.get("env"),
        "status": orchestrator_result_dict.get("status"),
        "message_date": context.current_utc_datetime.isoformat(),
        "correlation_id": orchestrator_result_dict.get("correlation_id"), # 🚨 Pobieramy z oryginalnego payloadu
        "manifest": silver_manifest_path,
        "summary_ingestion_uri": orchestrator_result_dict.get("summary_url")
    }

    yield context.call_activity("write_to_queue", {"payload": event_grid_payload})
    
    return orchestrator_result_dict


@app.activity_trigger(input_name="input")
async def run_bronze_orchestrator_activity(input: Dict[str, Any]) -> Dict[str, Any]:
    logger.info("run_bronze_orchestrator_activity started.")
    
    input_payload = input["input_payload"]
    
    try:
        config = ConfigManager()
        orchestrator = OrchestratorFactory.get_instance(ETLLayer.BRONZE, config=config)
        
        parser = BronzePayloadParser()
        bronze_context = parser.parse(input_payload)
        
        result = await orchestrator.run(bronze_context)

        return result.to_dict()

    except Exception as e:
        logger.exception(f"Błąd podczas uruchamiania BronzeOrchestrator: {e}")
        # Próba odtworzenia correlation_id w przypadku błędu
        correlation_id = input_payload.get("correlation_id", "NOT_PROVIDED")
        return {
            "status": "FAILED",
            "correlation_id": correlation_id,
            "message": str(e),
            "error_details": traceback.format_exc(),
        }


@app.activity_trigger(input_name="input")
async def write_to_queue(input: Dict[str, Any]):
    logger.info("write_to_queue activity started.")
    payload = input.get("payload")
    if not payload:
        return {"status": "FAILED", "message": "Brak payloadu."}

    endpoint = os.environ.get("EVENT_GRID_ENDPOINT")
    key = os.environ.get("EVENT_GRID_KEY")
    
    if not endpoint:
        error_message = "Zmienna środowiskowa 'EVENT_GRID_ENDPOINT' nie jest ustawiona."
        logger.error(error_message)
        return {"status": "FAILED", "message": error_message}
    
    try:
        manager = EventGridClientManager(endpoint=endpoint, key=key)
    except ValueError as e:
        logger.exception(f"Błąd inicjalizacji klienta Event Grid: {e}")
        return {"status": "FAILED", "message": f"Błąd inicjalizacji: {e}"}

    return manager.send_event(
        event_type="BronzeIngestionCompleted",
        subject=f"/silver/processing/{payload.get('correlation_id')}",
        data=payload
    )