# src/ingestion/api_clients/nobel_prize_api_client.py

from src.functions.common.api_clients.base_api_client import ApiClient
from typing import Dict, Any, AsyncGenerator, List 
import logging
import httpx 

from src.functions.common.api_clients.api_client_registry import ApiClientRegistry
from src.common.enums.domain_source import DomainSource

logger = logging.getLogger(__name__)

# Rejestracja pod wartością Enum, aby fabryka mogła odnaleźć tę klasę
@ApiClientRegistry.register(DomainSource.NOBELPRIZE.value) 
class NobelPrizeApiClient(ApiClient):
    """
    Klient API dla źródła danych Nobel Prize, obsługujący paginację typu offset/limit.
    """
    def __init__(self, config: Any):
        # Wywołujemy konstruktor klasy bazowej, przekazując tylko config
        super().__init__(config=config)
        self.default_limit = 50 
        self.base_api_endpoint = "nobelPrizes" 

    async def _fetch_single_page(self, params: Dict[str, Any]) -> httpx.Response:
        """
        Pobiera pojedynczą stronę danych z API Nobel Prize.
        """
        url = f"{self.base_url}{self.base_api_endpoint}" 
        try:
            response = await self.client.get(url, params=params, timeout=30.0)
            response.raise_for_status() 
            return response
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching Nobel Prize data page (status: {e.response.status_code}): {e.response.text}")
            raise 
        except httpx.RequestError as e:
            logger.error(f"Request error fetching Nobel Prize data page: {e}")
            raise 

    def _extract_records_from_response(self, response_json: Dict[str, Any]) -> List[Any]:
        """
        Wyodrębnia listę rekordów (nagród Nobla) z odpowiedzi JSON.
        """
        return response_json.get("nobelPrizes", [])

    async def fetch_all_records(self, initial_request_payload: Dict[str, Any]) -> AsyncGenerator[Any, None]:
        """
        Implementuje logikę paginacji dla Nobel Prize API (offset i limit).
        Generuje pojedyncze rekordy.
        """
        current_offset = initial_request_payload.get("offset", 0)
        current_limit = initial_request_payload.get("limit", self.default_limit)
        
        while True:
            params = {"offset": current_offset, "limit": current_limit}
            logger.info(f"Fetching Nobel Prize data page with params: {params}")
            
            response = await self._fetch_single_page(params)
            response_json = response.json()

            nobel_prizes = self._extract_records_from_response(response_json)
            
            for prize in nobel_prizes:
                yield prize 

            meta = response_json.get("meta", {})
            total_count = meta.get("count")

            if total_count is not None and (current_offset + current_limit) >= total_count:
                logger.info("Reached total count, no more Nobel Prize pages to fetch.")
                break 
            
            if not nobel_prizes: 
                logger.info("Current Nobel Prize page is empty, no more data.")
                break

            current_offset += current_limit 

    @property
    def base_url_setting_name(self) -> str:
        """
        Zwraca nazwę ustawienia w ConfigManagerze dla bazowego URL-a API Nagród Nobla.
        """
        return "NOBELPRIZE_API_BASE_URL"