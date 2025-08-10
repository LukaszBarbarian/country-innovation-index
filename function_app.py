from html import parser
import uuid
import azure.functions as func
import azure.durable_functions as df
import logging
import json
import os
import traceback
from typing import Dict, Any


from src.bronze.contexts.bronze_parser import BronzePayloadParser
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer

from src.bronze.init import bronze_init
from src.common.storage_account.queue_storage_manager import QueueStorageManager


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ---------------------------------------------------------------------
# 1) Queue trigger: odbiera wiadomość (payload — lista obiektów),
#    uruchamia Durable Orchestrator i przekazuje mu listę elementów.
# ---------------------------------------------------------------------
@app.function_name(name="IngestNowQueue")
@app.queue_trigger(arg_name="msg", queue_name="bronze-tasks", connection="AzureWebJobsStorageQueue")
@app.durable_client_input(client_name="starter")
async def ingest_now_queue(msg: func.QueueMessage, starter: df.DurableOrchestrationClient):
    try:
        body_bytes = msg.get_body()
        body_text = body_bytes.decode("utf-8")
        items_to_process = json.loads(body_text)

        if not isinstance(items_to_process, list) or not items_to_process:
            logger.error("Oczekiwano listy pozycji do przetworzenia na kolejce.")
            return  # nic nie robimy — można też wysłać DLQ itp.

        # start orchestrator and pass the whole list as input
        # start_new(orchestrator_function_name, instance_id=None, input=None)
        instance_id = await starter.start_new("ingest_orchestrator", None, items_to_process)
        logger.info(f"Rozpoczęto orkiestrację z ID = '{instance_id}' dla {len(items_to_process)} pozycji.")
        # opcjonalnie można zapisać instance_id na innej kolejce/logu
        return

    except json.JSONDecodeError:
        logger.exception("Nieprawidłowy JSON w wiadomości z kolejki.")
        return
    except Exception:
        logger.exception("Błąd podczas obsługi wiadomości z kolejki.")
        return

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
    queue_result = yield context.call_activity("write_to_silver_queue", summary_payload)

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

    correlation_id = input.get("correlation_id", str(uuid.uuid4()))

    try:
        parsed_context = BronzePayloadParser(correlation_id).parse(input)

        result = await OrchestratorFactory.get_instance(
            ETLLayer.BRONZE, config=config
        ).run(parsed_context)

        logger.info(f"Pomyślnie przetworzono pozycję: {getattr(result, 'correlation_id', 'UNKNOWN')}")
        # jeśli result jest modelem, zwracamy jego dict

        if result:
            return result.to_dict()
        
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
@app.activity_trigger(input_name="payload")
async def write_to_silver_queue(payload: Dict[str, Any]):
    try:
        queue_manager = QueueStorageManager("silver-processing-queue")
        queue_manager.send_message(json.dumps(payload))
        
        logger.info("Summary for Silver pipeline placed on queue.")
        return {"status": "ENQUEUED"}

    except Exception as e:
        logger.exception("Błąd podczas wysyłania summary na kolejkę.")
        return {"status": "FAILED", "message": str(e), "error_details": traceback.format_exc()}
