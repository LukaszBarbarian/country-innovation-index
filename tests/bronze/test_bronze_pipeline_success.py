import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.contexts.bronze_parser import BronzeParser
from src.bronze.orchestrator.bronze_orchestrator import BronzeOrchestrator
from src.common.azure_clients.event_grid_client_manager import EventGridNotifier
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer


# Przykładowy, uproszczony manifest
mock_manifest = {
    "env": "dev",
    "etl_layer": "bronze",
    "sources": [
        {
            "source_config_payload": {
                "domain_source_type": "api",
                "domain_source": "NOBELPRIZE",
                "dataset_name": "laureates",
                "request_payload": {"offset": 0, "limit": 100}
            }
        },
        {
            "source_config_payload": {
                "domain_source_type": "api",
                "domain_source": "WORLDBANK",
                "dataset_name": "population",
                "request_payload": {"indicator": "SP.POP.TOTL", "format": "json", "per_page": 1000}
            }
        }
    ]
}

@pytest.mark.asyncio
async def test_bronze_pipeline_success():
    context = BronzeContext(
    env=Env.DEV,
    etl_layer=ETLLayer.BRONZE,
    manifest=mock_manifest
)
    mock_config = MagicMock()

    orchestrator = BronzeOrchestrator(config=mock_config)

    with patch.object(BronzeParser, "parse", return_value=context):
        mock_result = AsyncMock()
        mock_result.status = "SUCCESS"
        mock_result.correlation_id = "test-123"
        mock_result.duration_in_ms = 100
        mock_result.summary_url = "http://fake-summary"
        mock_result.to_dict.return_value = {
            "status": "SUCCESS",
            "correlation_id": "test-123",
            "duration_in_ms": 100
        }
        orchestrator.execute = AsyncMock(return_value=mock_result)

        with patch.object(EventGridNotifier, "send_notification", return_value=None) as mock_notify:
            result = await orchestrator.execute(context)

            assert result.status == "SUCCESS"
            assert result.correlation_id == "test-123"
            mock_notify.assert_not_called()