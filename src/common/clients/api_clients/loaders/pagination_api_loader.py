from typing import Callable, Dict, Any, List, Optional
import httpx
import logging

from src.common.clients.api_clients.loaders.base_api_loader import ApiLoader
from src.common.models.raw_data import RawData

logger = logging.getLogger(__name__)

class PaginationApiLoader(ApiLoader):
    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        endpoint: str,
        initial_payload: Optional[Dict[str, Any]] = None,
        page_param: str = "offset",
        limit_param: str = "limit",
        default_limit: int = 50,
        extractor: Optional[Callable[[Dict[str, Any]], List[Any]]] = None,
    ):
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.endpoint = endpoint
        self.initial_payload = initial_payload or {}
        self.page_param = page_param
        self.limit_param = limit_param
        self.default_limit = default_limit
        self.extractor = extractor or (lambda r: [r])

    async def load(self) -> List[RawData]:
        offset = self.initial_payload.get(self.page_param, 0)
        limit = self.initial_payload.get(self.limit_param, self.default_limit)
        all_results: List[RawData] = []

        while True:
            params = dict(self.initial_payload)
            params[self.page_param] = offset
            params[self.limit_param] = limit

            url = f"{self.base_url}/{self.endpoint}"
            logger.info(f"Loading page from {url} with params {params}")

            response = await self.client.get(url, params=params)
            response.raise_for_status()
            json_data = response.json()

            records = self.extractor(json_data)
            if not records:
                logger.info(f"No records returned from endpoint {self.endpoint}, stopping pagination.")
                break

            all_results.append(RawData(
                data=records,
                dataset_name=self.endpoint
            ))

            if len(records) < limit:
                logger.info(f"Last page reached at offset {offset}, stopping pagination.")
                break

            offset += limit

        return all_results