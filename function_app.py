# src/functions/function_orchestrator.py
import azure.functions as func
import logging
import json
import os
import uuid
import traceback
from typing import Optional, Dict
from src.bronze.contexts.bronze_payload_parser import BronzePayloadParser
from src.common.models.orchestrator_result import OrchestratorResult
from src.bronze.orchestrators.bronze_orchestrator import BronzeOrchestrator
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer # Pamiętaj o imporcie, jeśli używasz enuma

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

    payload: Dict = {}
    correlation_id: str = 'UNKNOWN' 
    queue_message_id: str = 'UNKNOWN'
    api_name: str = 'UNKNOWN'
    dataset_name: str = 'UNKNOWN'
    env: str = 'UNKNOWN'
    etl_layer: str = "UNKNOWN"
    
    result: Optional[OrchestratorResult] = None

    try:
        payload = req.get_json()
        logger.info(f"Received ADF payload: {payload}")
    except ValueError:
        logger.error("Invalid JSON body received. Payload must be a valid JSON object.")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": "Invalid JSON body. Expected JSON payload."}),
            status_code=400,
            mimetype="application/json")

    try:
        try:
            bronze_context = BronzePayloadParser().parse(payload)
            
            correlation_id = bronze_context.correlation_id 
            queue_message_id = bronze_context.queue_message_id 
            env = bronze_context.env
            etl_layer = bronze_context.etl_layer
            api_name = bronze_context.api_name_str 
            dataset_name = bronze_context.dataset_name 

        except ValueError as ve:
            logger.error(f"Missing or invalid data in ADF payload: {ve}. Payload: {payload}")
            correlation_id = payload.get('correlation_id', 'UNKNOWN')
            queue_message_id = payload.get('queue_message_id', 'UNKNOWN')
            env = payload.get('env', 'UNKNOWN')
            etl_layer = payload.get('etl_layer', 'UNKNOWN')
            
            return func.HttpResponse(
                json.dumps({
                    "status": "FAILED",
                    "correlation_id": correlation_id,
                    "queue_message_id": queue_message_id,
                    "etl_layer": etl_layer,
                    "env": env,
                    "message": f"Invalid payload structure: {str(ve)}"
                }),
                status_code=400,
                mimetype="application/json")

        logger.info(f"""Invoking Bronze Orchestrator for API: {api_name},
                    Dataset: {dataset_name}, 
                    Correlation ID: {correlation_id}, 
                    Queue Message ID: {queue_message_id},
                    ETL Layer: {etl_layer},
                    Env: {env}
                    """)

        result = await bronze_orchestrator.run(bronze_context) 
        
        http_status_code = 200 if result.is_success else 500

        response_body = {
            "status": result.status,
            "correlation_id": result.correlation_id, 
            "queue_message_id": result.queue_message_id, 
            "api_name": result.api_name,
            "dataset_name": result.dataset_name,
            "layer_name": result.layer_name, 
            "env" : result.env,
            "message": result.message,
            "api_response_status_code": result.api_response_status_code,
            "output_path": result.output_path 
        }
        if result.is_failed and result.error_details:
            response_body["error_details"] = result.error_details

        return func.HttpResponse(
            json.dumps(response_body),
            status_code=http_status_code,
            mimetype="application/json"
        )
    
    except Exception as e:
        logger.exception(f"""Unhandled fatal error in IngestNow for 
                         Correlation ID: {correlation_id}, 
                         Queue Message ID: {queue_message_id},
                         ETL Layer: {etl_layer},
                         ENV: {env}""")
        
        if result is None: 
            error_details = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "stack_trace": traceback.format_exc()
            }

            result = OrchestratorResult(
                status="FAILED",
                correlation_id=correlation_id, 
                queue_message_id=queue_message_id,
                api_name=api_name,
                dataset_name=dataset_name,
                layer_name=ETLLayer.BRONZE.value,
                env=env,
                message=f"IngestNow function encountered an unhandled error: {str(e)}",
                error_details=error_details
            )

        return func.HttpResponse(
            json.dumps({
                "status": result.status,
                "correlation_id": result.correlation_id,
                "queue_message_id": result.queue_message_id,
                "api_name": result.api_name,
                "dataset_name": result.dataset_name,
                "layer_name": result.layer_name,
                "env": result.env,
                "message": result.message,
                "api_response_status_code": result.api_response_status_code,
                "output_path": result.output_path,
                "error_details": result.error_details
            }),
            status_code=500,
            mimetype="application/json"
        )
