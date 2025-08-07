# src/functions/function_orchestrator.py
import azure.functions as func
import logging
import json
import os
import uuid
import traceback
from typing import Optional, Dict
from src.common.contexts.payload_parser import PayloadParser
from src.common.enums.env import Env
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer
import src.bronze.init.bronze_init 


logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.function_name(name="IngestNow")
@app.route(route="ingestNow", methods=["POST"])
async def ingest_now(req: func.HttpRequest) -> func.HttpResponse:
    config = ConfigManager()

    payload: Dict = {}
    correlation_id: str = 'UNKNOWN' 
    queue_message_id: str = 'UNKNOWN'
    domain_source: str = 'UNKNOWN'
    domain_source_type: str = "UNKNOWN"
    dataset_name: str = 'UNKNOWN'
    env: Env = Env.UNKNOWN
    etl_layer: ETLLayer = ETLLayer.UNKNOWN
    http_status_code = 0
    
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
            context = PayloadParser().parse(payload)
            
            correlation_id = context.correlation_id 
            queue_message_id = context.queue_message_id 
            env = context.env
            etl_layer = context.etl_layer
            domain_source = context.domain_source.value 
            domain_source_type = context.domain_source_type.value
            dataset_name = context.dataset_name 

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
        
        
        result = await OrchestratorFactory\
            .get_instance(ETLLayer.BRONZE, config=config)\
            .run(context)
        
        http_status_code = 200 if result.is_success else 500

        response_body = {
            "status": result.status,
            "correlation_id": result.correlation_id, 
            "queue_message_id": result.queue_message_id, 
            "domain_source": result.domain_source.value,
            "domain_source_type" : result.domain_source_type.value,
            "dataset_name": result.dataset_name,
            "layer_name": result.layer_name.value, 
            "env" : result.env.value,
            "message": result.message,
            "source_response_status_code": result.source_response_status_code,
            "output_paths": result.output_paths if result.output_paths is not None else []
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
        
        error_details = None

        if result is None: 
            error_details = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "stack_trace": traceback.format_exc()
            }

            

        return func.HttpResponse(
            json.dumps({
                "status": "FAILED",
                "correlation_id": correlation_id,
                "queue_message_id": queue_message_id,
                "domain_source": domain_source,
                "domain_source_type": domain_source_type,
                "dataset_name": dataset_name,
                "layer_name": etl_layer.value if hasattr(etl_layer, 'value') else etl_layer,
                "env": env.value if hasattr(env, 'value') else env,
                "message": f"IngestNow function encountered an unhandled error: {str(e)}",
                "source_response_status_code": http_status_code,
                "output_paths": [],  # Zawsze lista, nawet pusta, zamiast output_path
                "error_details": error_details
            }),
            status_code=500,
            mimetype="application/json"
        )