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
        initial_payload: Dict[str, Any],
        page_param: str = "offset",
        limit_param: str = "limit",
        extractor: Optional[Callable[[Any], List[Any]]] = None,
    ):
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.endpoint = endpoint
        self.initial_payload = initial_payload  # już dict
        self.page_param = page_param
        self.limit_param = limit_param
        self.extractor = extractor or (lambda r: r.get("data", []) if isinstance(r, dict) else r)

    async def load(self) -> List[RawData]:
        current_page_or_offset = self.initial_payload.get(self.page_param, 0 if self.page_param == "offset" else 1)
        current_limit = self.initial_payload.get(self.limit_param, 100)
        all_results: List[RawData] = []

        while True:
            params = {**self.initial_payload, self.page_param: current_page_or_offset, self.limit_param: current_limit}
            url = f"{self.base_url}/{self.endpoint}"
            logger.info(f"Loading page from {url} with params {params}")

            try:
                response = await self.client.get(url, params=params)
                response.raise_for_status()
                json_data = response.json()

                records = self.extractor(json_data)
                
                if not records:
                    logger.info("No records returned, stopping pagination.")
                    break

                for record in records:
                    all_results.append(RawData(data=record, dataset_name=self.endpoint))
                
                # JEDYNA POTRZEBNA ZMIANA:
                current_page_or_offset += current_limit if self.page_param == "offset" else 1

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error: {e}")
                break
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")
                break

        return all_results