# src/functions/function_orchestrator.py
import azure.functions as func
import logging
import json
import os
from src.functions.common.models.ingestion_context import IngestionContext
from src.functions.common.ingestor.data_ingestor import DataIngestor
from src.functions.common.config.config_manager import ConfigManager
from src.common.storage_account.queue_storage_manager import QueueStorageManager

logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


QUEUE_NAME = os.getenv("QUEUE_NAME")
EVENT_GRID_TOPIC_ENDPOINT = os.environ.get('EVENTGRID_TOPIC_ENDPOINT')


@app.function_name(name="EnqueueTask")
@app.route(route="enqueueTask", methods=["POST"])
def EnqueueTask(req: func.HttpRequest) -> func.HttpResponse:
    
    try:
        req_body = req.get_json()
    except ValueError:
        logger.error("Invalid JSON body.")
        return func.HttpResponse(
            "Please pass a valid JSON body with 'api_name', 'dataset_name' and optionally 'api_request_payload'.",
            status_code=400
        )

    required_fields = ["api_name", "dataset_name"]
    if not all(field in req_body for field in required_fields):
        return func.HttpResponse(
            "Missing required fields in JSON body.",
            status_code=400
        )

    try:
        queue_manager = QueueStorageManager(queue_name=QUEUE_NAME)
        message = json.dumps(req_body)
        queue_manager.send_message(message_content=message)

        return func.HttpResponse("Task enqueued successfully.", status_code=200)

    except Exception as e:
        logger.exception("Failed to enqueue task.")
        return func.HttpResponse(
            f"Failed to enqueue task: {str(e)}",
            status_code=500
        )




@app.function_name(name="IngestFromQueue")
@app.queue_trigger(arg_name="msg", queue_name=QUEUE_NAME, connection="AzureWebJobsStorage")
def ingest_from_queue(msg: func.QueueMessage) -> None:
    config = ConfigManager()
    data_ingestor = DataIngestor(config)

    try:
        task = json.loads(msg.get_body().decode("utf-8"))
        ingestion_context = IngestionContext(
            api_name=task["api_name"],
            dataset_name=task["dataset_name"],
            api_request_payload=task.get("api_request_payload", {})
        )
        
        data_ingestor.ingest(ingestion_context)
    except Exception as e:
        logger.exception(f"Error ingesting data from queue: {e}")
