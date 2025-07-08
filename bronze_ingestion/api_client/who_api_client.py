import logging
from typing import Optional, Dict, Any

# Zaktualizowane importy, aby były względne do pakietu bronze_ingestion.api_client
from .base import ApiClient, ApiResponse 

# Importuj fabrykę i typy z pakietu bronze_ingestion.api_factory
from bronze_ingestion.api_factory.factory import ApiFactory
from bronze_ingestion.api_factory.types import ApiType, WhoApiQueryType # Upewnij się, że WhoApiQueryType jest tutaj zdefiniowane lub zaimportowane

logger = logging.getLogger(__name__)


class WhoApiResponse(ApiResponse):
    """
    Specjalizowana klasa ApiResponse dla WHO API, która wyodrębnia dane z klucza 'value'.
    """
    def __init__(self, base_api_response: ApiResponse):
        # Przekopiuj podstawowe atrybuty z bazowej odpowiedzi
        self.status_code = base_api_response.status_code
        self.headers = base_api_response.headers
        self.text = base_api_response.text
        self.ok = base_api_response.ok
        self.raw_response = base_api_response.raw_response # Ważne, aby zachować oryginalną odpowiedź requests, jeśli jest

        raw_data = base_api_response.data
        
        # Logika specyficzna dla WHO API: wydobywanie danych z klucza 'value'
        if isinstance(raw_data, dict) and 'value' in raw_data:
            self.data = raw_data['value']
            logger.debug("WhoApiResponse: Extracted 'value' from WHO API response data.")
        elif self.ok and raw_data is not None:
            self.data = raw_data
            logger.warning("WhoApiResponse: Response data did not contain 'value' key, using full JSON data.")
        else:
            self.data = raw_data
            logger.warning(f"WhoApiResponse: Base ApiResponse not OK or data is None. Status: {self.status_code}")

    def json(self) -> Any:
        """Zwraca przetworzone dane w formacie JSON (czyli wartość klucza 'value')."""
        return self.data

    def __str__(self):
        data_len_str = str(len(self.data)) if isinstance(self.data, (list, dict)) else 'N/A'
        return f"WhoApiResponse(status={self.status_code}, ok={self.ok}, data_len={data_len_str})"


@ApiFactory.register_api_client(ApiType.WHO)
class WhoApiClient(ApiClient):
    # Base URL jest stały dla tego klienta, przekazujemy go do konstruktora klasy bazowej
    BASE_API_URL = "https://ghoapi.azureedge.net/api"

    def __init__(self):
        # Wywołaj konstruktor klasy bazowej, przekazując base_url
        super().__init__(base_url=WhoApiClient.BASE_API_URL)
        logger.info(f"WhoApiClient initialized with base_url: {self.base_url}")

    def fetch_data(
        self,
        query_type: WhoApiQueryType, 
        indicator: Optional[str] = None,
        dimension_name: Optional[str] = None, 
        filters: Optional[Dict[str, str]] = None
    ) -> ApiResponse:
        """
        Pobiera dane z WHO API na podstawie podanych parametrów.
        Buduje endpoint i parametry zapytania zgodnie ze specyfikacją WHO GHO API.
        """
        endpoint = ""
        query_params: Dict[str, str] = {}

        if query_type == "indicator_data":
            if not indicator:
                raise ValueError("Indicator ID is required for 'indicator_data' query type.")
            endpoint = indicator # Dla WHO GHO API, ID wskaźnika jest bezpośrednio endpointem
        elif query_type == "dimensions":
            endpoint = "Dimension" # Endpoint do listy wszystkich wymiarów
        elif query_type == "dimension_values":
            if not dimension_name:
                raise ValueError("Dimension name (e.g., 'COUNTRY') is required for 'dimension_values' query type.")
            endpoint = f"DIMENSION/{dimension_name}/DimensionValues" # Endpoint do listy wartości dla konkretnego wymiaru
        elif query_type == "indicators": # Zgodnie z tym co miałeś '_indicators' to 'Indicator' endpoint
            endpoint = "Indicator" # Endpoint do listy wszystkich wskaźników
        else:
            raise ValueError(f"Unsupported WHO API query type: {query_type}")

        # Dodaj filtry OData do query_params
        if filters:
            filter_parts = []
            for dim, value in filters.items():
                if dim.endswith("_contains"): # Obsługa filtra 'contains'
                    real_dim = dim[:-len("_contains")]
                    filter_parts.append(f"contains({real_dim},'{value}')")
                else: # Standardowy filtr 'equal'
                    filter_parts.append(f"{dim} eq '{value}'")
            query_params["$filter"] = " and ".join(filter_parts)

        logger.info(f"Fetching WHO data (type: {query_type}) from endpoint: {endpoint} with query params: {query_params}")
        
        # Użyj metody `get` z klasy bazowej `ApiClient` do wykonania zapytania HTTP
        # `super().get` zapewni, że używana jest sesja requests z klasy bazowej
        base_api_response: ApiResponse = super().get(endpoint=endpoint, params=query_params)

        # Przetwórz bazową odpowiedź za pomocą WhoApiResponse, aby wyodrębnić dane 'value'
        who_api_response = WhoApiResponse(base_api_response)
        logger.info("WHO API response processed by WhoApiResponse.")
        
        return who_api_response