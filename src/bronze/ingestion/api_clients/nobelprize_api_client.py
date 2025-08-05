# src/ingestion/api_clients/nobel_prize_api_client.py

from src.common.api_clients.base_api_client import ApiClient
from typing import Dict, Any,  List 
import logging
from src.common.api_clients.pagination_api_loader import PaginationApiLoader
from src.common.models.api_result import ApiResult
from src.common.registers.api_client_registry import ApiClientRegistry
from src.common.enums.domain_source import DomainSource

logger = logging.getLogger(__name__)


@ApiClientRegistry.register(DomainSource.NOBELPRIZE) 
class NobelPrizeApiClient(ApiClient):
    def __init__(self, config: Any):
        super().__init__(config=config, base_url_setting_name="NOBEL_API_BASE_URL")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_all(self, payload: Dict[str, Any]) -> List[ApiResult]:
        results: List[ApiResult] = []

        # Loader dla nagród
        prizes_loader = PaginationApiLoader(
            client=self.client,
            base_url=self.base_url,
            limit_param="limit",
            page_param="offset",
            endpoint="nobelPrizes",
            initial_payload=payload,
            extractor=lambda r: [r], 
        )
        results.extend(await prizes_loader.load())

        # Loader dla laureatów
        laureates_loader = PaginationApiLoader(
            client=self.client,
            base_url=self.base_url,
            limit_param="limit",
            page_param="offset",
            endpoint="laureates",
            initial_payload=payload,
            extractor=lambda r: [r], 
        )
        results.extend(await laureates_loader.load())

        return results