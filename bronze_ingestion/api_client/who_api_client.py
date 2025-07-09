import logging
from typing import Optional, Dict, Any

# Zaktualizowane importy, aby były względne do pakietu bronze_ingestion.api_client
from .base import ApiClient, ApiResponse 

# Importuj fabrykę i typy z pakietu bronze_ingestion.api_factory
from bronze_ingestion.api_factory.factory import ApiFactory
from bronze_ingestion.api_factory.types import ApiType

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
    indicator: Optional[str] = None,
    dimension: Optional[str] = None,
    filters: Optional[Dict[str, str]] = None) -> ApiResponse:
        endpoint = ""
        query_params: Dict[str, str] = {}

        if indicator:
            if indicator.lower() == "all":
                endpoint = "Indicator"
            else:
                endpoint = indicator

        elif dimension:
            if dimension.lower() == "all":
                endpoint = "Dimension"
            else:
                endpoint = f"DIMENSION/{dimension}/DimensionValues"

        else:
            raise ValueError("At least one of 'indicator' or 'dimension' must be provided.")

        # Filtry OData
        if filters:
            filter_parts = []
            for dim, value in filters.items():
                if dim.endswith("_contains"):
                    real_dim = dim[:-len("_contains")]
                    filter_parts.append(f"contains({real_dim},'{value}')")
                else:
                    filter_parts.append(f"{dim} eq '{value}'")
            query_params["$filter"] = " and ".join(filter_parts)

        logger.info(f"Fetching WHO data from endpoint: {endpoint} with query params: {query_params}")
        base_api_response = super().get(endpoint=endpoint, params=query_params)
        return WhoApiResponse(base_api_response)
