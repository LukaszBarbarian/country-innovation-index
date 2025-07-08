# bronze_ingestion/api_handlers/who_api_handler.py

import azure.functions as func
import logging
from typing import Dict, Tuple, Union
from bronze_ingestion.api_factory.types import WhoApiQueryType 

logger = logging.getLogger(__name__)

async def prepare_who_ingestion_params(req: func.HttpRequest) -> Union[Tuple[Dict, str], func.HttpResponse]:
    """
    Parsuje parametry zapytania HTTP dla WHO API, waliduje je
    i przygotowuje słowniki do wywołania API oraz nazwę datasetu do zapisu.

    Args:
        req (func.HttpRequest): Obiekt żądania HTTP z Azure Function.

    Returns:
        Union[Tuple[Dict, str], func.HttpResponse]:
            - Krotka (api_params, dataset_name) w przypadku sukcesu walidacji.
            - func.HttpResponse z kodem błędu w przypadku nieudanej walidacji parametrów żądania.
    """
    logger.info('Starting parameter parsing for WHO API ingestion request.')

    # --- 1. Parsowanie parametru 'query' z żądania HTTP ---
    query_str = req.params.get("query")
    
    if not query_str:
        logger.warning("Handler Error: Missing 'query' query parameter.")
        return func.HttpResponse(
            "Please provide 'query' as a query parameter (e.g., ?query=indicator_data).",
            status_code=400
        )
    
    # --- 2. Walidacja parametru 'query' względem dopuszczalnych typów ---
    # Upewnij się, że WhoApiQueryType.__args__ działa poprawnie z Twoją definicją Literal
    if query_str not in WhoApiQueryType.__args__: 
        logger.warning(f"Handler Error: Invalid 'query_type': '{query_str}'. Expected one of: {', '.join(WhoApiQueryType.__args__)}")
        return func.HttpResponse(
            f"Invalid 'query_type': '{query_str}'. Expected one of: {', '.join(WhoApiQueryType.__args__)}",
            status_code=400
        )
    
    query: WhoApiQueryType = query_str

    # --- 3. Parsowanie pozostałych opcjonalnych parametrów ---
    indicator = req.params.get("indicator")
    dimension_name = req.params.get("dimension_name")

    # --- 4. Walidacja zależnych parametrów na podstawie typu zapytania ---
    if query == "indicator_data" and not indicator:
        logger.warning("Handler Error: For 'query=indicator_data', 'indicator' parameter is required.")
        return func.HttpResponse(
            "For 'query=indicator_data', 'indicator' parameter is required.",
            status_code=400
        )
    if query == "dimension_values" and not dimension_name: # Zgodnie z WhoApiQueryType "dimension_values"
        logger.warning("Handler Error: For 'query=dimension_values', 'dimension_name' parameter is required.")
        return func.HttpResponse(
            "For 'query=dimension_values', 'dimension_name' parameter is required.",
            status_code=400
        )

    # --- 5. Parsowanie dynamicznych filtrów (parametry zaczynające się od "filter_") ---
    filters: Dict[str, str] = {}
    for param_name, param_value in req.params.items():
        if param_name.startswith("filter_"):
            dimension_key = param_name[len("filter_"):]
            if dimension_key:
                filters[dimension_key] = param_value
                logger.debug(f"Handler Debug: Found filter: {dimension_key}={param_value}")

    # --- 6. Tworzenie słownika parametrów API do przekazania klientowi API ---
    api_params = {
        "query_type": query, # Zmieniono nazwę klucza z "query" na "query_type", aby pasował do WhoApiClient.fetch_data
        "indicator": indicator,
        "dimension_name": dimension_name,
        "filters": filters
    }

    # --- 7. Budowanie nazwy zbioru danych dla storage ---
    # Nazwa datasetu powinna być unikalna dla każdej kombinacji parametrów
    dataset_name = f"whoapi_{query.replace('_', '-')}"
    if indicator:
        dataset_name += f"_{indicator.lower()}"
    if dimension_name:
        dataset_name += f"_{dimension_name.lower()}"
    if filters:
        sorted_filter_keys = sorted(filters.keys())
        filter_str = "_".join([f"{k.lower()}_{filters[k].lower().replace(' ', '-')}" for k in sorted_filter_keys])
        dataset_name += f"_{filter_str}"

    logger.info(f"Handler Success: Parameters prepared for WHO API. Query Type: {query}. Dataset name: {dataset_name}")
    
    # --- 8. Zwrócenie przygotowanych danych lub odpowiedzi HTTP z błędem ---
    return api_params, dataset_name