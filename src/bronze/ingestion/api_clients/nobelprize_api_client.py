# src/ingestion/api_clients/nobel_prize_api_client.py

from typing import Dict, Any,  List 
import logging
from src.bronze.contexts.bronze_layer_context import BronzeLayerContext
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


    async def fetch_all(self, context: BronzeLayerContext) -> List[RawData]:
        dataset_name = context.dataset_name
       
        logger.info(f"Fetching dataset '{dataset_name}' from NobelPrize API...")

        loader = PaginationApiLoader(
            client=self.client,
            base_url=self.base_url,
            limit_param="limit",
            page_param="offset",
            endpoint=dataset_name, 
            initial_payload=context.request_payload,
            extractor=lambda r: [r],
        )
        return await loader.load()