import uuid
import json
import os
import traceback
import logging
from typing import Dict, Any

import azure.functions as func
import azure.durable_functions as df

# Importy z Twoich modułów
from src.bronze.contexts.bronze_parser import BronzePayloadParser
from src.common.azure_clients.event_grid_client_manager import EventGridClientManager
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer

from src.bronze.init import bronze_init

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)



@app.route(route="HttpTrigger1")
def HttpTrigger1(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(
            f"Hello, {name}. This HTTP triggered function executed successfully.",
            mimetype="text/plain"
        )
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            mimetype="text/plain"
        )
