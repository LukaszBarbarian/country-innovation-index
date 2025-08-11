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
# 1) Queue trigger: odbiera wiadomość (payload — lista obiektów),
#    uruchamia Durable Orchestrator i przekazuje mu listę elementów.
#    Ta funkcja pozostaje niezmieniona, służy do wyzwalania z kolejki.
# ---------------------------------------------------------------------
@app.function_name(name="ingest_now_queue")
@app.queue_trigger(arg_name="msg", queue_name="bronze-tasks", connection="AzureWebJobsStorageQueue")
@app.durable_client_input(client_name="starter")
async def ingest_now_queue(msg: func.QueueMessage, starter: df.DurableOrchestrationClient):
    try:
        body_bytes = msg.get_body()
        body_text = body_bytes.decode("utf-8")
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
# NOWA FUNKCJA HTTP:
# 1.a) HTTP trigger: odbiera payload (listę obiektów) jako ciało żądania HTTP.
#      Ta funkcja jest punktem wejścia dla Azure Data Factory (ADF).
# ---------------------------------------------------------------------
@app.function_name(name="start_ingestion_http")
@app.route(route="start_ingestion")
@app.durable_client_input(client_name="starter")
async def start_ingestion_http(req: func.HttpRequest, starter: df.DurableOrchestrationClient) -> func.HttpResponse:
    logger.info('HTTP trigger function processed a request.')

    try:
        # POBIERAMY SUROWĄ ZAWARTOŚĆ CIAŁA ŻĄDANIA
        req_body_bytes = req.get_body()
        req_body_text = req_body_bytes.decode("utf-8")
        
        # LOGUJEMY SUROWĄ ZAWARTOŚĆ ŻĄDANIA, ABY ZOBACZYĆ, CO DOKŁADNIE PRZYSŁAŁ ADF
        logger.info(f"Surowa zawartość żądania: {req_body_text}")

        # PONIŻEJ PRÓBUJEMY PARSOWAĆ JSON
        req_body = req.get_json()

    except ValueError:
        logger.exception("Błąd: Wymagany jest poprawny format JSON w ciele żądania.")
        return func.HttpResponse(
            "Błąd: Wymagany jest poprawny format JSON w ciele żądania.",
            status_code=400
        )
    except Exception as e:
        logger.exception(f"Inny błąd podczas przetwarzania żądania: {e}")
        return func.HttpResponse(
            f"Błąd: {str(e)}",
            status_code=500
        )

    items_to_process = req_body

    if not isinstance(items_to_process, list) or not items_to_process:
        logger.error("Oczekiwano listy pozycji do przetworzenia w ciele żądania.")
        return func.HttpResponse(
            "Błąd: Oczekiwano niepustej listy pozycji do przetworzenia.",
            status_code=400
        )


    try:
        # Uruchomienie Twojego istniejącego Durable Orchestratora
        instance_id = await starter.start_new("ingest_orchestrator", None, items_to_process)
        logger.info(f"Rozpoczęto orkiestrację z ID = '{instance_id}' dla {len(items_to_process)} pozycji.")

        return func.HttpResponse(
            f"Orkiestracja dla {len(items_to_process)} pozycji została rozpoczęta. ID: {instance_id}",
            status_code=202  # 202 Accepted oznacza, że żądanie zostało przyjęte do przetworzenia
        )
    except Exception as e:
        logger.exception(f"Błąd podczas uruchamiania orkiestracji: {e}")
        return func.HttpResponse(
            f"Błąd podczas uruchamiania orkiestracji: {e}",
            status_code=500
        )


# ---------------------------------------------------------------------
# 2) Orchestrator: wykonuje równolegle aktywności dla każdego elementu
#    z listy, czeka na wszystkie wyniki, składa summary i wywołuje
#    aktywność, która wrzuci summary na kolejkę silver.
# ---------------------------------------------------------------------
@app.orchestration_trigger(context_name="context")
def ingest_orchestrator(context: df.DurableOrchestrationContext):
    items = context.get_input()

    if not isinstance(items, list) or not items:
        return {
            "status": "FAILED",
            "message": "Orchestrator received an empty or invalid list of items."
        }

    correlation_id = str(uuid.uuid4())

    # fan-out: dla każdego itemu uruchamiamy aktywność RunIngestionActivity
    tasks = []
    for item in items:
        # Dodaj correlation_id do każdego elementu z listy
        item['correlation_id'] = correlation_id
        tasks.append(context.call_activity("run_ingestion_activity", item))
    
    results = yield context.task_all(tasks)

    # tworzymy summary
    summary_payload = {
        "status": "BRONZE_COMPLETED",
        "correlation_id": correlation_id,
        "timestamp": context.current_utc_datetime.isoformat(),
        "processed_items": len(items),
        "results": results
    }

    # wywołujemy aktywność, która zapisze summary na kolejce "silver-processing-queue"
    queue_result = yield context.call_activity("write_to_queue", { "payload": summary_payload, "queue_name": "silver-processing-queue"})

    return {
        "status": "COMPLETED",
        "bronze_results": results,
        "correlation_id": correlation_id,
        "silver_queue_status": queue_result
    }

# ---------------------------------------------------------------------
# 3) Activity: przetwarza pojedynczy payload (BRONZE). Zwraca dict z outcome.
# ---------------------------------------------------------------------
@app.activity_trigger(input_name="input")
async def run_ingestion_activity(input: Dict[str, Any]) -> Dict[str, Any]:
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
# 4) Activity: otrzymuje summary (payload) i wysyła je bezpośrednio na
#    storage queue (używamy SDK, nie dekoratora queue_output).
# ---------------------------------------------------------------------
# @app.activity_trigger(input_name="input")
# async def write_to_queue(input: Dict[str, Any]):
#     queue_name = ""

#     try:
#         payload = input.get("payload")
#         queue_name = input.get("queue_name")

#         if not payload or not queue_name:
#             return {"status": "FAILED", "message": "Brak payloadu lub nazwy kolejki."}

#         queue_manager = QueueStorageManager(queue_name)
#         queue_manager.send_message(json.dumps(payload))

#         logger.info(f"Summary for pipeline placed on queue '{queue_name}'.")
#         return {"status": "ENQUEUED", "queue": queue_name}

#     except Exception as e:
#         logger.exception(f"Błąd podczas wysyłania summary na kolejkę '{queue_name}'.")
#         return {"status": "FAILED", "message": str(e), "error_details": traceback.format_exc()}


@app.activity_trigger(input_name="input")
async def write_to_queue(input: Dict[str, Any]):
    try:
        payload = input.get("payload")

        if not payload:
            return {"status": "FAILED", "message": "Brak payloadu."}

        # --- ZMIANA: ZAMIENIAMY WYSYŁANIE NA KOLEJKĘ NA EVENT GRID ---
        #
        # Wczytaj endpoint i klucz z ustawień aplikacji
        endpoint = os.environ["EVENT_GRID_ENDPOINT"]
        key = os.environ["EVENT_GRID_KEY"]

        credential = AzureKeyCredential(key)
        client = EventGridPublisherClient(endpoint, credential)

        # Tworzymy zdarzenie
        event = EventGridEvent(
            subject=f"/silver/processing/{payload.get('correlation_id')}",
            event_type="SilverProcessing.Started",
            data=payload,
            data_version="1.0"
        )

        # Wysyłamy zdarzenie do Event Grid
        client.send([event])

        logger.info(f"Zdarzenie do Event Grid wysłane dla correlation_id: {payload.get('correlation_id')}.")
        return {"status": "EVENT_SENT", "event_grid_topic": endpoint}
        #
        # -----------------------------------------------------------

    except Exception as e:
        logger.exception(f"Błąd podczas wysyłania zdarzenia do Event Grid.")
        return {"status": "FAILED", "message": str(e), "error_details": traceback.format_exc()}