# src/functions/function_orchestrator.py
import azure.functions as func
import logging
import json
import os
import uuid
import traceback # Upewnij się, że jest zaimportowany
from typing import Optional, Dict # Dodaj Dict do importów

from src.common.contexts.bronze_context import BronzeContext
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.orchestrators.bronze_orchestrator import BronzeOrchestrator
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

    # Inicjalizacja zmiennych dla obsługi błędów i logowania, 
    # muszą być dostępne w całym bloku funkcji
    payload: Dict = {}
    correlation_id: str = 'UNKNOWN' 
    queue_message_id: str = 'UNKNOWN'
    api_name: str = 'UNKNOWN'
    dataset_name: str = 'UNKNOWN'
    
    result: Optional[OrchestratorResult] = None # Zmienna do przechowywania wyniku z orkiestratora

    # --- Blok try dla parsowania początkowego payloadu JSON ---
    try:
        payload = req.get_json()
        logger.info(f"Received ADF payload: {payload}")
    except ValueError:
        logger.error("Invalid JSON body received. Payload must be a valid JSON object.")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": "Invalid JSON body. Expected JSON payload."}),
            status_code=400,
            mimetype="application/json")
    
    # --- Główny blok try-except dla całej logiki biznesowej ---
    try:
        # Próba utworzenia BronzeContext - walidacja struktury payloadu ADF
        try:
            bronze_context = BronzeContext.from_payload(payload=payload)
            
            correlation_id = bronze_context.correlation_id 
            queue_message_id = bronze_context.queue_message_id 
            api_name = bronze_context.api_name_str 
            dataset_name = bronze_context.dataset_name 

        except ValueError as ve:
            logger.error(f"Missing or invalid data in ADF payload: {ve}. Payload: {payload}")
            # Pobieramy ID z surowego payloadu, jeśli są dostępne
            correlation_id = payload.get('correlation_id', 'UNKNOWN')
            queue_message_id = payload.get('queue_message_id', 'UNKNOWN')
            
            # Zwracamy błąd HTTP, ponieważ payload jest niepoprawny dla kontekstu
            return func.HttpResponse(
                json.dumps({
                    "status": "FAILED",
                    "correlationId": correlation_id,
                    "queueMessageId": queue_message_id,
                    "message": f"Invalid payload structure: {str(ve)}"
                }),
                status_code=400,
                mimetype="application/json")

        logger.info(f"Invoking Bronze Orchestrator for API: {api_name}, Dataset: {dataset_name}, Correlation ID: {correlation_id}, Queue Message ID: {queue_message_id}")

        # Wywołanie BronzeOrchestrator i odebranie OrchestratorResult
        result = await bronze_orchestrator.run(bronze_context) 
        
        # Przygotowanie odpowiedzi HTTP na podstawie OrchestratorResult
        http_status_code = 200 if result.is_success else 500

        # Budowanie ciała odpowiedzi JSON (camelCase dla ADF)
        response_body = {
            "status": result.status,
            "correlationId": result.correlation_id, 
            "queueMessageId": result.queue_message_id, 
            "apiName": result.api_name,
            "datasetName": result.dataset_name,
            "layerName": result.layer_name, 
            "message": result.message,
            "apiResponseStatusCode": result.api_response_status_code,
            "outputPath": result.output_path 
        }
        # Dodajemy szczegóły błędu tylko wtedy, gdy operacja się nie powiodła
        if result.is_failed and result.error_details:
            response_body["errorDetails"] = result.error_details

        logger.info(f"IngestNow returning status {result.status} for Correlation ID: {correlation_id}, Queue Message ID: {queue_message_id}")
        return func.HttpResponse(
            json.dumps(response_body),
            status_code=http_status_code,
            mimetype="application/json"
        )
    
    except Exception as e:
        # Obsługa wszelkich innych nieprzewidzianych błędów w głównym bloku logiki
        logger.exception(f"Unhandled fatal error in IngestNow for Correlation ID: {correlation_id}, Queue Message ID: {queue_message_id}.")
        
        # Jeśli 'result' nie został jeszcze utworzony (błąd nastąpił bardzo wcześnie),
        # tworzymy minimalny OrchestratorResult do zwrócenia
        if result is None: 
            error_details = {
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc()
            }
            # Upewniamy się, że layer_name jest zdefiniowane
            result = OrchestratorResult(
                status="FAILED",
                correlation_id=correlation_id, 
                queue_message_id=queue_message_id,
                api_name=api_name, # Używamy już pobranych nazw, jeśli są dostępne
                dataset_name=dataset_name,
                layer_name=ETLLayer.BRONZE.value, # Użyj wartości enuma
                message=f"IngestNow function encountered an unhandled error: {str(e)}",
                error_details=error_details
            )

        # W przypadku błędu zawsze zwracamy status 500
        return func.HttpResponse(
            json.dumps({
                "status": result.status,
                "correlationId": result.correlation_id,
                "queueMessageId": result.queue_message_id,
                "apiName": result.api_name,
                "datasetName": result.dataset_name,
                "layerName": result.layer_name,
                "message": result.message,
                "apiResponseStatusCode": result.api_response_status_code,
                "outputPath": result.output_path,
                "errorDetails": result.error_details
            }),
            status_code=500,
            mimetype="application/json"
        )