# src/ingestion/api_clients/world_bank_api_client.py

from typing import Dict, Any, List
import logging
from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
from src.common.clients.api_clients.base_api_client import ApiClient
from src.common.clients.api_clients.loaders.pagination_api_loader import PaginationApiLoader
from src.common.config.config_manager import ConfigManager
from src.common.models.ingestion_context import IngestionContext
from src.common.models.raw_data import RawData
from src.common.registers.api_client_registry import ApiClientRegistry
from src.common.enums.domain_source import DomainSource

logger = logging.getLogger(__name__)


@ApiClientRegistry.register(DomainSource.WORLDBANK)
class WorldBankApiClient(ApiClient):
    def __init__(self, config: ConfigManager):
        super().__init__(config=config, base_url_setting_name="WORLDBANK_API_BASE_URL")

    async def fetch_all(self, context: IngestionContext) -> List[RawData]:
        """
        Pobiera wszystkie dane z World Bank API na podstawie IngestionContext.
        request_payload jest traktowany bezpośrednio jako słownik parametrów.
        """
        dataset_name = context.source_config.dataset_name
        request_payload = context.source_config.request_payload  # dict lub RequestConfig

        logger.info(f"Fetching dataset '{dataset_name}' from World Bank API...")

        indicator = request_payload.get("indicator")
        if not indicator:
            raise ValueError("Indicator not found in request_payload for World Bank API.")

        endpoint = f"country/all/indicator/{indicator}"

        request_payload = {**request_payload, "page": 1}
        query_params = {k: v for k, v in request_payload.items() if k != "indicator"}        


        loader = PaginationApiLoader(
            client=self.client,
            base_url=self.base_url,
            limit_param="per_page",
            page_param="page",
            endpoint=endpoint,
            initial_payload=query_params,  # przekazujemy słownik bez .params
            extractor=lambda r: self._world_bank_extractor(r)
        )

        return await loader.load()
    

    def _world_bank_extractor(self, response_json):
        # Sprawdzamy, czy odpowiedź jest listą, ma co najmniej dwa elementy i drugi element jest listą.
        if isinstance(response_json, list) and len(response_json) > 1 and isinstance(response_json[1], list):
            # Zwracamy czystą listę słowników (surowych danych), bez tworzenia obiektów RawData.
            return response_json[1]
        return []
