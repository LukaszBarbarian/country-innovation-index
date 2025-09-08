from typing import List
import logging
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.models.manifest import BronzeManifestSource
from src.common.clients.api_clients.base_api_client import ApiClient
from src.common.clients.api_clients.loaders.pagination_api_loader import PaginationApiLoader
from src.common.config.config_manager import ConfigManager
from src.common.models.raw_data import RawData
from src.common.registers.api_client_registry import ApiClientRegistry
from src.common.enums.domain_source import DomainSource

logger = logging.getLogger(__name__)


@ApiClientRegistry.register(DomainSource.WORLDBANK)
class WorldBankApiClient(ApiClient):
    def __init__(self, config: ConfigManager):
        super().__init__(config=config, base_url_setting_name="WORLDBANK_API_BASE_URL")

    async def fetch_all(self, manifest_source: BronzeManifestSource) -> List[RawData]:
        """
        Pobiera wszystkie dane z World Bank API na podstawie BronzeContext.
        Wyszukuje konfigurację źródła w manifestcie po domain_source i dataset_name.
        """
        # Znajdź konfigurację dla World Bank w manifestcie
        source_config = manifest_source.source_config_payload


        if not source_config:
            logger.warning("No source configuration found for WORLDBANK in manifest.")
            return []

        dataset_name = source_config.dataset_name
        request_payload = source_config.request_payload

        indicator = request_payload.get("indicator")
        if not indicator:
            raise ValueError(f"Indicator not found in request_payload for dataset '{dataset_name}'.")

        endpoint = f"country/all/indicator/{indicator}"
        query_params = {k: v for k, v in request_payload.items() if k != "indicator"}
        query_params["page"] = 1  # startujemy od pierwszej strony

        logger.info(f"Fetching dataset '{dataset_name}' from World Bank API...")

        loader = PaginationApiLoader(
            client=self.client,
            base_url=self.base_url,
            limit_param="per_page",
            page_param="page",
            endpoint=endpoint,
            initial_payload=query_params,
            extractor=self._world_bank_extractor
        )

        return await loader.load()

    @staticmethod
    def _world_bank_extractor(response_json):
        """
        Wyciąga dane z odpowiedzi World Bank API.
        Oczekujemy listy, gdzie drugi element jest listą rekordów.
        """
        if isinstance(response_json, list) and len(response_json) > 1 and isinstance(response_json[1], list):
            return response_json[1]
        return []
