import datetime
import logging
import json
import os
import traceback
from typing import Any, Dict
import uuid
import azure.functions as func
import azure.durable_functions as df

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
