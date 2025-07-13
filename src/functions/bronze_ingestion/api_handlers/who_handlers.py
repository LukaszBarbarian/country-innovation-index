import azure.functions as func
import logging
import json
from typing import Dict, Tuple, Union

async def prepare_who_ingestion_params(req: func.HttpRequest) -> Union[Tuple[Dict, str], func.HttpResponse]:
    indicator = req.params.get("indicator")
    dimension = req.params.get("dimension")

    if req.method == 'POST':
        try:
            req_body = req.get_json()
            if req_body:
                if "indicator" in req_body and not indicator:
                    indicator = req_body["indicator"]
                if "dimension" in req_body and not dimension:
                    dimension = req_body["dimension"]

        except ValueError:
            logging.error("Could not parse request body as JSON in prepare_who_ingestion_params.")
            pass
        except Exception as e:
            logging.error(f"Error reading request body in prepare_who_ingestion_params: {e}")
            pass

    if not indicator and not dimension:
        return func.HttpResponse(
            "You must provide at least 'indicator' or 'dimension' parameter, either in query or body.",
            status_code=400
        )

    api_params = {
        "indicator": indicator,
        "dimension": dimension
    }

    dataset_name = "whoapi"
    if indicator:
        dataset_name += f"_indicator_{indicator.lower()}"
    if dimension:
        dataset_name += f"_dimension_{dimension.lower()}"    

    return api_params, dataset_name