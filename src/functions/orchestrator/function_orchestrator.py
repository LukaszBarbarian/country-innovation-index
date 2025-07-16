# src/functions/function_orchestrator.py
import azure.functions as func
import logging
import json

from ..common.models.ingestion_context import IngestionContext
from ..ingestion.data_ingestor import DataIngestor
from ..common.config_manager import ConfigManager
from ..common.logging_config import setup_logging # Ustawienie logowania

setup_logging()
logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="ingestData", methods=["GET", "POST"])
async def ingest_data_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("Ingest Data HTTP trigger function received a request.")
    config = ConfigManager() # Odczyt konfiguracji (np. z Key Vault)

    try:
        req_body = req.get_json()
    except ValueError:
        logger.error("Invalid JSON body.")
        return func.HttpResponse(
            "Please pass a JSON body with 'api_name', 'dataset_name' and 'params' (optional).",
            status_code=400
        )

    api_name = req_body.get('api_name')
    dataset_name = req_body.get('dataset_name')
    api_params = req_body.get('params', {})
    
    if not api_name or not dataset_name:
        logger.error("Missing 'api_name' or 'dataset_name' in request body.")
        return func.HttpResponse(
            "Please provide 'api_name' and 'dataset_name' in the request body.",
            status_code=400
        )

    try:
        # Utwórz kontekst ingestii
        ingestion_context = IngestionContext(
            api_name=api_name,
            dataset_name=dataset_name,
            request_params=api_params
        )

        # Wstrzyknij zależności do DataIngestor
        data_ingestor = DataIngestor(config) # DataIngestor przejmuje odpowiedzialność

        # Uruchom proces ingestii
        blob_path = await data_ingestor.ingest(ingestion_context)

        response_payload = {
            "status": "SUCCESS",
            "message": f"Data ingested successfully for {api_name}/{dataset_name}.",
            "blob_path": blob_path
        }
        return func.HttpResponse(json.dumps(response_payload), mimetype="application/json")

    except Exception as e:
        logger.exception(f"An error occurred during data ingestion: {e}")
        error_payload = {
            "status": "ERROR",
            "message": f"Failed to ingest data: {str(e)}"
        }
        return func.HttpResponse(json.dumps(error_payload), mimetype="application/json", status_code=500)