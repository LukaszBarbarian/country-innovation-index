from typing import Callable, Dict, Any, List, Optional
import httpx
import logging
from dataclasses import asdict

from src.common.clients.api_clients.loaders.base_api_loader import ApiLoader
from src.common.models.manifest import ApiRequestPayload
from src.common.models.raw_data import RawData

logger = logging.getLogger(__name__)

class PaginationApiLoader(ApiLoader):
    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        endpoint: str,
        initial_payload: ApiRequestPayload,
        page_param: str = "offset",
        limit_param: str = "limit",
        extractor: Optional[Callable[[Dict[str, Any]], List[Any]]] = None,
    ):
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.endpoint = endpoint
        self.initial_payload = initial_payload
        self.page_param = page_param
        self.limit_param = limit_param
        # Domyślne wartości są już w ApiRequestPayload
        self.extractor = extractor or (lambda r: r.get("data", []) if isinstance(r, dict) else r)

    async def load(self) -> List[RawData]:
        # Poprawne pobranie początkowych wartości z initial_payload
        current_offset = self.initial_payload.offset
        current_limit = self.initial_payload.limit
        all_results: List[RawData] = []

        while True:
            # Tworzymy słownik parametrów ręcznie, korzystając z bieżących wartości
            params = {
                self.page_param: current_offset,
                self.limit_param: current_limit
            }

            url = f"{self.base_url}/{self.endpoint}"
            logger.info(f"Loading page from {url} with params {params}")

            try:
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

                if len(records) < current_limit:
                    logger.info(f"Last page reached at offset {current_offset}, stopping pagination.")
                    break

                # Aktualizacja offsetu do następnej strony
                current_offset += current_limit
            
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
                break
            except Exception as e:
                logger.error(f"An unexpected error occurred during API request: {e}")
                break

        return all_results