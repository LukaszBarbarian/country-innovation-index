import datetime
import logging
import json
import os
import traceback
from typing import Any, Dict
import uuid
import azure.functions as func
import azure.durable_functions as df
from src.common.enums.etl_layers import ETLLayer

from src.bronze.init import bronze_init

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
logger = logging.getLogger(__name__)


@app.route(route="start_ingestion")
@app.durable_client_input(client_name="starter")
async def start_ingestion_http(req: func.HttpRequest, starter: df.DurableOrchestrationClient) -> func.HttpResponse:
    """
    HTTP trigger to start the data ingestion orchestration.
    """
    try:
        manifest_payload = req.get_json()
    except ValueError:
        logger.error("Request does not contain valid JSON.")
        return func.HttpResponse("Error: Expected a valid JSON format.", status_code=400)
    except Exception as e:
        logger.exception(f"Failed to process request: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    
    try:
        instance_id = await starter.start_new("ingest_orchestrator", None, manifest_payload)
        logger.info(f"Started orchestration with ID = '{instance_id}'.")
        return starter.create_check_status_response(req, instance_id)
    except Exception as e:
        logger.exception(f"Failed to start orchestration: {e}")
        return func.HttpResponse(f"Error starting orchestration: {e}", status_code=500)



@app.orchestration_trigger(context_name="context")
def ingest_orchestrator(context: df.DurableOrchestrationContext):
    """
    Durable orchestration to manage the bronze layer data ingestion process.

    It calls the 'run_ingestion_activity' to execute the main ingestion logic and then
    the 'write_to_queue' activity to publish an event to Event Grid based on the result.
    """
    input_payload = context.get_input()

    if not isinstance(input_payload, dict) or not input_payload:
        logger.error("Orchestrator received an empty or invalid payload.")
        return {"status": "FAILED", "message": "Orchestrator received an empty or invalid payload."}
    
    orchestrator_result_dict = yield context.call_activity(
        "run_ingestion_activity", 
        {"input_payload": input_payload}
    )

    logger.info(f"Bronze orchestration completed. Result: {orchestrator_result_dict.get('status', 'N/A')}")
   
    return orchestrator_result_dict

