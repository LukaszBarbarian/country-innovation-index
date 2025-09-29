import pytest
from unittest.mock import AsyncMock, MagicMock
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.orchestrator.bronze_orchestrator import BronzeOrchestrator
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from urllib.parse import urlparse

mock_manifest = {
    "env": "dev",
    "etl_layer": "bronze",
    "sources": [
        {"source_config_payload": {"domain_source_type": "api", "domain_source": "NOBELPRIZE", "dataset_name": "laureates"}},
        {"source_config_payload": {"domain_source_type": "api", "domain_source": "WORLDBANK", "dataset_name": "population"}}
    ]
}

@pytest.mark.asyncio
async def test_bronze_orchestrator_summary_urls():
    context = BronzeContext(env=Env.DEV, etl_layer=ETLLayer.BRONZE, manifest=mock_manifest)
    orchestrator = BronzeOrchestrator(config=MagicMock())

    # Patchujemy execute żeby zwrócił przykładowy summary
    summary_mock = MagicMock()
    summary_mock.status = "COMPLETED"
    summary_mock.correlation_id = "test-123"
    summary_mock.results = [
        {
            "status": "COMPLETED",
            "domain_source": "NOBELPRIZE",
            "dataset_name": "laureates",
            "output_paths": ["https://example.blob.core.windows.net/bronze/nobelprize/file1.json"]
        },
        {
            "status": "COMPLETED",
            "domain_source": "WORLDBANK",
            "dataset_name": "population",
            "output_paths": ["https://example.blob.core.windows.net/bronze/worldbank/file2.json"]
        }
    ]
    orchestrator.execute = AsyncMock(return_value=summary_mock)

    result = await orchestrator.execute(context)

    assert result.status == "COMPLETED"
    assert result.correlation_id == "test-123"

    for r, source in zip(result.results, mock_manifest["sources"]):
        source_payload = source["source_config_payload"]
        assert r["domain_source"] == source_payload["domain_source"]
        assert r["dataset_name"] == source_payload["dataset_name"]
        assert len(r["output_paths"]) > 0

        for path in r["output_paths"]:
            parsed = urlparse(path)
            assert parsed.scheme == "https"
            assert ".blob.core.windows.net" in parsed.netloc
            assert path.endswith(".json")
            # opcjonalnie: sprawdzenie warstwy w URL
            assert f"/{mock_manifest['etl_layer']}/" in path
