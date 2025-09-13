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
    """
    A specific API client for fetching data from the World Bank API.

    This class extends `ApiClient` and is registered with `ApiClientRegistry`
    for the `DomainSource.WORLDBANK`, making it discoverable and usable
    within the data ingestion framework.
    """
    def __init__(self, config: ConfigManager):
        """
        Initializes the WorldBankApiClient.

        Args:
            config (ConfigManager): The configuration manager used to retrieve
                                    the base URL for the World Bank API.
        """
        super().__init__(config=config, base_url_setting_name="WORLDBANK_API_BASE_URL")

    async def fetch_all(self, manifest_source: BronzeManifestSource) -> List[RawData]:
        """
        Fetches all data for a specific indicator from the World Bank API.

        This method extracts the `indicator` from the manifest's request payload
        to construct the correct API endpoint. It then uses the `PaginationApiLoader`
        to handle data retrieval, including the API's specific pagination parameters.

        Args:
            manifest_source (BronzeManifestSource): An object containing configuration
                                                    details, including the specific
                                                    indicator to fetch.

        Returns:
            List[RawData]: A list of `RawData` objects containing the fetched data.

        Raises:
            ValueError: If the required `indicator` is not found in the manifest's
                        request payload.
        """
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
        query_params["page"] = 1

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
        A static method that extracts the relevant data from a World Bank API response.

        The World Bank API typically returns a list where the second element is the
        list of actual data records. This method handles that specific response format.

        Args:
            response_json (Any): The JSON response from the World Bank API.

        Returns:
            list: The list of data records, or an empty list if the response format is unexpected.
        """
        if isinstance(response_json, list) and len(response_json) > 1 and isinstance(response_json[1], list):
            return response_json[1]
        return []