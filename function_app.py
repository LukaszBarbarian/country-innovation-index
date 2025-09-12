import datetime
import logging
import json
import os
import traceback
from typing import Any, Dict
import uuid
import azure.functions as func
import azure.durable_functions as df
from src.bronze.contexts.bronze_parser import BronzeParser
from src.common.azure_clients.event_grid_client_manager import EventGridClientManager
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer

import src.bronze.init.bronze_init 

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


@app.activity_trigger(input_name="input")
async def run_ingestion_activity(input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Activity function to execute the bronze layer data ingestion logic AND
    publish an event to Event Grid.
    """
    config = ConfigManager()
    
    input_payload = input["input_payload"]

    correlation_id = input_payload.get("correlation_id", str(uuid.uuid4()))
    logger.info(f"Running ingestion for correlation ID: {correlation_id}")
    
    result = None
    try:
        orchestrator = OrchestratorFactory.get_instance(ETLLayer.BRONZE, config=config)
        parser = BronzeParser(config)
        
        bronze_context = parser.parse(input_payload)
        bronze_context.correlation_id = correlation_id
        
        result = await orchestrator.execute(bronze_context)
        return result.to_dict()

    except Exception as e:
        logger.exception(f"BronzeOrchestrator failed for correlation ID {correlation_id}.")
        result_dict = {
            "status": "FAILED",
            "correlation_id": correlation_id,
            "message": str(e),
            "error_details": traceback.format_exc(),
        }
        return result_dict
    
    finally:
        # Ten blok wykona się zawsze, niezależnie od tego, czy wystąpił błąd
        # To kluczowe, aby wysłać powiadomienie o sukcesie LUB porażce
        try:
            silver_manifest_path = f"/silver/manifest/{input_payload.get("env")}.manifest.json"
            
            event_grid_payload = {
                "layer": ETLLayer.BRONZE.value,
                "env": input_payload.get("env"),
                "status": result.status,
                "message_date": datetime.datetime.utcnow,
                "correlation_id": result.correlation_id,
                "manifest": silver_manifest_path,
                "summary_ingestion_uri": result.summary_url,
                "duration_in_ms": result.duration_in_ms
            }

            endpoint = config.get("EVENT_GRID_ENDPOINT")
            key = config.get("EVENT_GRID_KEY")
            
            if endpoint:
                manager = EventGridClientManager(endpoint=endpoint, key=key)
                manager.send_event(
                    event_type="BronzeIngestionCompleted",
                    subject=f"/silver/processing/{result.correlation_id}",
                    data=event_grid_payload
                )
                logger.info(f"Event for correlation ID {result.correlation_id} sent successfully.")
            else:
                logger.warning("Event Grid endpoint not configured. Skipping notification.")

        except Exception as e:
            logger.exception(f"Failed to send Event Grid notification: {e}")