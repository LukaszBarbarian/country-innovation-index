import httpx
import pytest
from src.common.clients.api_clients.loaders.pagination_api_loader import PaginationApiLoader

@pytest.mark.asyncio
async def test_pagination_loader_builds_correct_payload(aiohttp_server):
    async with httpx.AsyncClient() as client:
        loader = PaginationApiLoader(
            client=client,
            base_url="http://example.com",
            endpoint="data",
            initial_payload={"offset": 0, "limit": 100, "indicator": "SP.POP.TOTL"}
        )

        # Patch client.get, żeby sprawdzić co dostaje
        async def fake_get(url, params):
            assert params["offset"] == 0
            assert params["limit"] == 100
            assert params["indicator"] == "SP.POP.TOTL"
            # symulujemy pustą odpowiedź, żeby loader się zatrzymał
            class FakeResponse:
                def raise_for_status(self): return None
                def json(self): return {"data": []}
            return FakeResponse()

        loader.client.get = fake_get

        results = await loader.load()
        assert results == []  # nic nie zwróciło, ale payload był sprawdzony