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
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer
from src.bronze.init import bronze_init
from src.common.storage_account.queue_storage_manager import QueueStorageManager
from azure.core.credentials import AzureKeyCredential
from azure.eventgrid import EventGridPublisherClient, EventGridEvent

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ---------------------------------------------------------------------
# 1) Queue trigger
# ---------------------------------------------------------------------
@app.function_name(name="ingest_now_queue")
@app.queue_trigger(arg_name="msg", queue_name="bronze-tasks", connection="AzureWebJobsStorageQueue")
@app.durable_client_input(client_name="starter")
async def ingest_now_queue(msg: func.QueueMessage, starter: df.DurableOrchestrationClient):
    try:
        logger.info("ingest_now_queue triggered.")
        body_bytes = msg.get_body()
        body_text = body_bytes.decode("utf-8")
        logger.info(f"Message body text: {body_text}")

        items_to_process = json.loads(body_text)

        if not isinstance(items_to_process, list) or not items_to_process:
            logger.error("Oczekiwano listy pozycji do przetworzenia na kolejce.")
            return

        instance_id = await starter.start_new("ingest_orchestrator", None, items_to_process)
        logger.info(f"Rozpoczęto orkiestrację z ID = '{instance_id}' dla {len(items_to_process)} pozycji.")
        return

    except json.JSONDecodeError:
        logger.exception("Nieprawidłowy JSON w wiadomości z kolejki.")
        return
    except Exception:
        logger.exception("Błąd podczas obsługi wiadomości z kolejki.")
        return

# ---------------------------------------------------------------------
# 1.a) HTTP trigger
# ---------------------------------------------------------------------
@app.function_name(name="start_ingestion_http")
@app.route(route="start_ingestion")
@app.durable_client_input(client_name="starter")
async def start_ingestion_http(req: func.HttpRequest, starter: df.DurableOrchestrationClient) -> func.HttpResponse:
    logger.info('start_ingestion_http function triggered.')

    try:
        req_body_bytes = req.get_body()
        req_body_text = req_body_bytes.decode("utf-8")
        logger.info(f"Surowa zawartość żądania: {req_body_text}")

        req_body = req.get_json()
    except Exception as e:
        logger.exception(f"Inny błąd podczas przetwarzania żądania: {e}")
        return func.HttpResponse(
            f"Błąd: {str(e)}. Req: {str(req)}",
            status_code=500
        )

    items_to_process = req_body
    logger.info(f"Parsed JSON payload with {len(items_to_process) if isinstance(items_to_process, list) else 'unknown'} items.")

    if not isinstance(items_to_process, list) or not items_to_process:
        logger.error("Oczekiwano listy pozycji do przetworzenia w ciele żądania.")
        return func.HttpResponse(
            "Błąd: Oczekiwano niepustej listy pozycji do przetworzenia.",
            status_code=400
        )

    try:
        instance_id = await starter.start_new("ingest_orchestrator", None, items_to_process)
        logger.info(f"Rozpoczęto orkiestrację z ID = '{instance_id}' dla {len(items_to_process)} pozycji.")

        return func.HttpResponse(
            f"Orkiestracja dla {len(items_to_process)} pozycji została rozpoczęta. ID: {instance_id}",
            status_code=202
        )
    except Exception as e:
        logger.exception(f"Błąd podczas uruchamiania orkiestracji: {e}")
        return func.HttpResponse(
            f"Błąd podczas uruchamiania orkiestracji: {e}",
            status_code=500
        )

# ---------------------------------------------------------------------
# 2) Orchestrator
# ---------------------------------------------------------------------
@app.orchestration_trigger(context_name="context")
def ingest_orchestrator(context: df.DurableOrchestrationContext):
    logger.info("ingest_orchestrator started.")
    items = context.get_input()

    if not isinstance(items, list) or not items:
        logger.error("Orchestrator received empty or invalid list of items.")
        return {
            "status": "FAILED",
            "message": "Orchestrator received an empty or invalid list of items."
        }

    correlation_id = str(uuid.uuid4())
    logger.info(f"Generated correlation_id: {correlation_id}")

    tasks = []
    for item in items:
        item['correlation_id'] = correlation_id
        logger.info(f"Scheduling run_ingestion_activity for item with correlation_id: {correlation_id}")
        tasks.append(context.call_activity("run_ingestion_activity", item))

    results = yield context.task_all(tasks)
    logger.info(f"Completed ingestion activities. Results: {results}")

    summary_payload = {
        "status": "BRONZE_COMPLETED",
        "correlation_id": correlation_id,
        "timestamp": context.current_utc_datetime.isoformat(),
        "processed_items": len(items),
        "results": results
    }

    logger.info(f"Calling write_to_queue activity with summary payload.")
    queue_result = yield context.call_activity("write_to_queue", {"payload": summary_payload, "queue_name": "silver-processing-queue"})
    logger.info(f"write_to_queue result: {queue_result}")

    return {
        "status": "COMPLETED",
        "bronze_results": results,
        "correlation_id": correlation_id,
        "silver_queue_status": queue_result
    }

# ---------------------------------------------------------------------
# 3) Activity: run_ingestion_activity
# ---------------------------------------------------------------------
@app.activity_trigger(input_name="input")
async def run_ingestion_activity(input: Dict[str, Any]) -> Dict[str, Any]:
    logger.info(f"run_ingestion_activity started with input: {input}")

    config = ConfigManager()
    correlation_id = input.get("correlation_id")

    if not correlation_id:
        error_message = "Brak wymaganej wartości 'correlation_id' w payloadzie."
        logger.error(error_message)
        return {
            "status": "FAILED",
            "correlation_id": "NOT_PROVIDED",
            "message": error_message,
            "error_details": "Payload nie zawiera klucza 'correlation_id'.",
        }

    try:
        parsed_context = BronzePayloadParser(correlation_id).parse(input)
        logger.info(f"Parsed context for correlation_id {correlation_id}")

        result = await OrchestratorFactory.get_instance(
            ETLLayer.BRONZE, config=config
        ).run(parsed_context)

        logger.info(f"Pomyślnie przetworzono pozycję: {correlation_id}")

        if result:
            result_dict = result.to_dict()
            result_dict["correlation_id"] = correlation_id
            return result_dict

        return {"status": "OK", "correlation_id": correlation_id}

    except Exception as e:
        logger.exception(f"Błąd podczas przetwarzania pozycji {correlation_id}: {e}")
        return {
            "status": "FAILED",
            "correlation_id": correlation_id,
            "message": str(e),
            "error_details": traceback.format_exc()
        }

# ---------------------------------------------------------------------
# 4) Activity: write_to_queue - wysyłanie na Event Grid z loggingiem
# ---------------------------------------------------------------------
@app.activity_trigger(input_name="input")
async def write_to_queue(input: Dict[str, Any]):
    logger.info("write_to_queue activity started.")
    try:
        payload = input.get("payload")
        logger.info(f"Payload received for write_to_queue: {payload}")

        if not payload:
            logger.error("Brak payloadu w write_to_queue.")
            return {"status": "FAILED", "message": "Brak payloadu."}

        endpoint = os.environ.get("EVENT_GRID_ENDPOINT")
        key = os.environ.get("EVENT_GRID_KEY")

        if not endpoint or not key:
            error_msg = "Brak ustawionych zmiennych środowiskowych EVENT_GRID_ENDPOINT lub EVENT_GRID_KEY."
            logger.error(error_msg)
            return {"status": "FAILED", "message": error_msg}

        logger.info(f"EVENT_GRID_ENDPOINT: {endpoint}")

        credential = AzureKeyCredential(key)
        client = EventGridPublisherClient(endpoint, credential)

        event = EventGridEvent(
            subject=f"/silver/processing/{payload.get('correlation_id')}",
            event_type="BronzeIngestionCompleted",
            data=payload,
            data_version="1.0"
        )

        logger.info(f"Sending event to Event Grid for correlation_id: {payload.get('correlation_id')}")
        client.send([event])
        logger.info("Event Grid send succeeded.")

        return {"status": "EVENT_SENT", "event_grid_topic": endpoint}

    except Exception as e:
        logger.exception(f"Błąd podczas wysyłania zdarzenia do Event Grid: {e}")
        return {"status": "FAILED", "message": str(e), "error_details": traceback.format_exc()}
