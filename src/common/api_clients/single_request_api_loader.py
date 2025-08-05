from typing import Callable, Dict, Any, List
from src.common.api_clients.base_api_loader import ApiLoader
from src.common.models.api_result import ApiLoaderResult
import httpx
import logging

logger = logging.getLogger(__name__)

class StaticApiLoader(ApiLoader):
    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        endpoint: str,
        payload: Dict[str, Any],
        extractor: Callable[[Dict[str, Any]], List[Any]],
    ):
        self.client = client
        self.base_url = base_url
        self.endpoint = endpoint
        self.payload = payload
        self.extractor = extractor

    async def load(self) -> ApiLoaderResult:
        full_url = f"{self.base_url}/{self.endpoint}"
        logger.debug(f"Fetching static data from {full_url} with payload: {self.payload}")
        response = await self.client.get(full_url, params=self.payload)
        response.raise_for_status()
        data = response.json()
        records = self.extractor(data)

        return ApiLoaderResult(
            records=records,
            endpoint=self.endpoint,
            url=full_url,
            record_count=len(records),
        )