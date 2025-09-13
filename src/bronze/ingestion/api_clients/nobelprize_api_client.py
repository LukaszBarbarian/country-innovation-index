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
    """
    A concrete implementation of `ApiClient` designed to interact with the Nobel Prize API.
    This client is registered with the `ApiClientRegistry` for the `DomainSource.NOBELPRIZE`.
    """
    def __init__(self, config: ConfigManager):
        """
        Initializes the NobelPrizeApiClient.

        Args:
            config (ConfigManager): The configuration manager for retrieving application settings.
        """
        super().__init__(config=config, base_url_setting_name="NOBELPRIZE_API_BASE_URL")

    async def fetch_all(self, manifest_source: BronzeManifestSource) -> List[RawData]:
        """
        Fetches all data from the Nobel Prize API based on the provided manifest source.

        This method retrieves the specific configuration for the Nobel Prize data from
        the manifest, then uses a `PaginationApiLoader` to handle the fetching process,
        including pagination logic.

        Args:
            manifest_source (BronzeManifestSource): A manifest object containing the
                                                    configuration details for the data source.

        Returns:
            List[RawData]: A list of `RawData` objects containing the fetched API data.
                           Returns an empty list if no source configuration is found.
        """
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
            initial_payload=request_payload,
            extractor=lambda r: r.get(f"{dataset_name}", [])
        )

        return await loader.load()