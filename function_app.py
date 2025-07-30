# src/functions/function_orchestrator.py
import azure.functions as func
import logging
import json
import os
import uuid
import traceback
from src.common.contexts.bronze_context import BronzeContext
from src.common.orchestrators.bronze_orchestrator import BronzeOrchestrator
from src.common.config.config_manager import ConfigManager

logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


QUEUE_NAME = os.getenv("QUEUE_NAME")
EVENT_GRID_TOPIC_ENDPOINT = os.environ.get('EVENTGRID_TOPIC_ENDPOINT')
EVENTGRID_TOPIC_RESOURCE_ID = os.environ.get('EVENTGRID_TOPIC_RESOURCE_ID')


@app.function_name(name="IngestNow")
@app.route(route="ingestNow", methods=["POST"])
async def ingest_now(req: func.HttpRequest) -> func.HttpResponse:
    config = ConfigManager()


    bronze_orchestrator = BronzeOrchestrator(config=config)

    try:
        payload = req.get_json()
    except ValueError:
        logger.error("Invalid JSON body.")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": "Invalid JSON body."}),
            status_code=400,
            mimetype="application/json")

    required_fields = ["api_name", "dataset_name"]
    if not all(field in payload for field in required_fields):
        return func.HttpResponse(
            json.dumps({"status": "error", "message": "Missing required fields."}),
            status_code=400,
            mimetype="application/json")    

    try:
        correlation_id = payload.get("correlation_id", str(uuid.uuid4()))

        bronze_context = BronzeContext(
            api_name_str=payload["api_name"],
            dataset_name=payload["dataset_name"],
            api_request_payload=payload.get("api_request_payload", {}),
            correlation_id=correlation_id
        )

    
        await bronze_orchestrator.run(bronze_context)

        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "correlation_id": correlation_id,
                "message": "Ingestion completed."
            }),
            status_code=200,
            mimetype="application/json"
        )
    

    except Exception as e:
        logger.exception("Fatal error during ingestion.")

        error_details = {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "stack_trace": traceback.format_exc()
        }

        return func.HttpResponse(
            json.dumps({
                "status": "failed",
                "correlation_id": correlation_id,
                "message": str(e),
                "error": error_details
            }),
            status_code=500,
            mimetype="application/json"
        )
