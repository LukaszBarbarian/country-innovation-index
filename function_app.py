import logging
import json
import azure.functions as func
import azure.durable_functions as df
from src.bronze.contexts.bronze_parser import BronzePayloadParser
from src.common.azure_clients.event_grid_client_manager import EventGridClientManager
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer

from src.bronze.init import bronze_init

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)






# 1. Funkcja HTTP Trigger
# Rozpoczyna orkiestrację po wywołaniu HTTP POST
@app.route(route="start_simple_orchestration")
@app.durable_client_input(client_name="starter")
async def start_simple_orchestration_http(req: func.HttpRequest, starter: df.DurableOrchestrationClient) -> func.HttpResponse:
    logging.info('HTTP trigger function processed a request to start orchestration.')
    
    # Rozpoczynamy orkiestrację o nazwie 'simple_orchestrator'
    instance_id = await starter.start_new("simple_orchestrator", None, {"name": "World"})
    
    logging.info(f"Started orchestration with ID = '{instance_id}'.")
    return starter.create_check_status_response(req, instance_id)

# 2. Funkcja Orchestrator
# Koordynuje wykonanie funkcji 'activity'
@app.orchestration_trigger(context_name="context")
def simple_orchestrator(context: df.DurableOrchestrationContext):
    logging.info("Starting simple orchestrator.")
    
    # Wywołujemy funkcję 'activity' o nazwie 'simple_activity' i czekamy na jej wynik
    result = yield context.call_activity("simple_activity", "World")
    
    logging.info(f"Orchestrator finished. Result: {result}")
    
    # Orkiestrator musi zwrócić wynik
    return result

# 3. Funkcja Activity
# Wykonuje faktyczną pracę
@app.activity_trigger(input_name="name")
def simple_activity(name: str):
    logging.info(f"Running simple activity with input: {name}")
    return f"Hello, {name} from the activity function!"