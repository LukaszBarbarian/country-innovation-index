from typing import Callable, Dict, Any, List, Optional
import httpx
import logging

from src.common.clients.api_clients.loaders.base_api_loader import ApiLoader
from src.common.exceptions.retry import async_retry
from src.common.models.raw_data import RawData

logger = logging.getLogger(__name__)

class PaginationApiLoader(ApiLoader):
    """
    A concrete implementation of ApiLoader that handles loading data from paginated APIs.
    It automatically iterates through pages until all data is retrieved.
    """
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
        """
        Initializes the paginated API loader.

        Args:
            client (httpx.AsyncClient): The asynchronous HTTP client to use for requests.
            base_url (str): The base URL of the API.
            endpoint (str): The specific API endpoint to call.
            initial_payload (Dict[str, Any]): The initial parameters to send with the first request.
            page_param (str): The name of the query parameter for the page/offset. Defaults to "offset".
            limit_param (str): The name of the query parameter for the page size. Defaults to "limit".
            extractor (Optional[Callable]): A function to extract the list of records from the API response.
                                            Defaults to a function that looks for a "data" key.
        """
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.endpoint = endpoint
        self.initial_payload = initial_payload
        self.page_param = page_param
        self.limit_param = limit_param
        self.extractor = extractor or (lambda r: r.get("data", []) if isinstance(r, dict) else r)


    @async_retry(max_retries=3, delay=2.0, exceptions=(httpx.HTTPStatusError, httpx.RequestError))
    async def _fetch_page(self, url: str, params: dict):
        response = await self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()



    async def load(self) -> List[RawData]:
        current_page_or_offset = self.initial_payload.get(self.page_param, 0 if self.page_param == "offset" else 1)
        current_limit = self.initial_payload.get(self.limit_param, 100)
        all_results: List[RawData] = []

        while True:
            params = {**self.initial_payload, self.page_param: current_page_or_offset, self.limit_param: current_limit}
            url = f"{self.base_url}/{self.endpoint}"
            logger.info(f"Loading page from {url} with params {params}")

            try:
                json_data = await self._fetch_page(url, params)
            except Exception as e:
                logger.error(f"Failed to load page {current_page_or_offset}: {e}")
                raise

            records = self.extractor(json_data)
            if not records:
                logger.info("No records returned, stopping pagination.")
                break

            for record in records:
                all_results.append(RawData(data=record, dataset_name=self.endpoint))

            current_page_or_offset += current_limit if self.page_param == "offset" else 1

        return all_results