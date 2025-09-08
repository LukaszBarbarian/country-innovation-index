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


@ApiClientRegistry.register(DomainSource.NOBELPRIZE)
class NobelPrizeApiClient(ApiClient):
    def __init__(self, config: ConfigManager):
        super().__init__(config=config, base_url_setting_name="NOBELPRIZE_API_BASE_URL")

    async def fetch_all(self, manifest_source: BronzeManifestSource) -> List[RawData]:
        """
        Pobiera wszystkie dane z NobelPrize API na podstawie BronzeContext.
        Wyszukuje konfigurację źródła w manifestcie po domain_source i dataset_name.
        """
        # Znajdź konfigurację dla tego źródła w manifestcie
        source_config = manifest_source.source_config_payload

        if not source_config:
            logger.warning("No source configuration found for NOBELPRIZE in manifest.")
            return []

        dataset_name = source_config.dataset_name
        request_payload = source_config.request_payload

        logger.info(f"Fetching dataset '{dataset_name}' from NobelPrize API...")

        loader = PaginationApiLoader(
            client=self.client,
            base_url=self.base_url,
            limit_param="limit",
            page_param="offset",
            endpoint=dataset_name,
            initial_payload=request_payload,  # przekazujemy słownik bez .params
            extractor=lambda r: r.get(f"{dataset_name}", [])
        )

        return await loader.load()
