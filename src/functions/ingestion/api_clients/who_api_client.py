# src/ingestion/api_clients/who_api_client.py
import requests
from urllib.parse import urlencode, quote_plus
from ...common.api_clients.base_api_client import ApiClient
from typing import Dict, Any, Optional
import logging
from src.common.enums.file_format import FileFormat
from src.functions.common.api_clients.api_client_registry import ApiClientRegistry
from src.common.enums.domain_source import DomainSource

logger = logging.getLogger(__name__)


@ApiClientRegistry.register(DomainSource.WHO)
class WhoApiClient(ApiClient):
    def __init__(self, config: Any):
        super().__init__(config)
        self.base_url = self.config.get_setting(self.base_url_setting_name)

    @property
    def api_identifier(self) -> str:
        return "who"

    async def fetch_data(self, api_request_payload: Dict[str, Any]) -> Any:
        """
        Pobiera dane z WHO API, interpretując api_request_payload.
        Oczekuje, że payload będzie zawierał 'endpoint_path' i opcjonalnie 'query_params'.
        """
        endpoint_path = api_request_payload.get('endpoint_path')
        query_params = api_request_payload.get('query_params', {})
        
        if not endpoint_path:
            raise ValueError("WHO API request payload must contain 'endpoint_path'.")

        full_url = f"{self.base_url}/{endpoint_path}"
        
        if query_params:
            encoded_params = urlencode(query_params, quote_via=quote_plus)
            full_url = f"{full_url}?{encoded_params}"
            
        logger.info(f"Fetching data from WHO API: {full_url}")

        try:
            response = requests.get(full_url)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from WHO API: {e}")
            raise

    @property
    def default_file_format(self) -> FileFormat:
        return FileFormat.JSON

    @property
    def base_url_setting_name(self) -> str:
        return "WHO_API_BASE_URL"

