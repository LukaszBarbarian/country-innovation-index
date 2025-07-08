import azure.functions as func
import logging
from shared.decorators.ingest_decorator import ingest_data_pipeline 
from bronze_ingestion.api_factory.types import ApiType
from bronze_ingestion.api_handlers.who_handlers import prepare_who_ingestion_params # Zmieniono nazwę pliku z 'who_handlers' na 'who_api_handler' zgodnie z konwencją

# --- Inicjalizacja loggera dla tego modułu ---
logger = logging.getLogger(__name__)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="whoingest2", methods=["GET", "POST"]) # Dodano wymagane metody HTTP
@ingest_data_pipeline(ApiType.WHO)
async def whoingest(req: func.HttpRequest) -> func.HttpResponse: # Funkcja musi być async
    """
    Punkt końcowy HTTP do inicjowania procesu ingestii danych z WHO API.
    Deleguje parsowanie i walidację parametrów do dedykowanego handlera,
    a następnie przekazuje wynik do ogólnego pipeline'u ingestii.
    """
    logger.info('Python HTTP trigger function "whoingest" received a request.')
    logger.info(f"Request method: {req.method}")
    logger.info(f"Request parameters: {req.params}")

    ingestion_data_or_response = await prepare_who_ingestion_params(req)

    if isinstance(ingestion_data_or_response, func.HttpResponse):
        logger.warning(f"Parameter preparation failed, returning error response with status: {ingestion_data_or_response.status_code}")
        return ingestion_data_or_response
    
    api_params, dataset_name = ingestion_data_or_response

    logger.info(f"Parameters prepared. Passing to ingestion pipeline. Dataset name: {dataset_name}")
    
    return api_params, dataset_name