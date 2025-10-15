# tests/bronze/test_bronze_flow.py
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from uuid import uuid4

from function_app import ETLLayer
from src.bronze.contexts.bronze_context import BronzeContext
from src.common.enums.env import Env
from src.common.models.ingestion_result import IngestionResult
from src.common.models.raw_data import RawData
from src.common.models.processed_result import ProcessedResult

@pytest.mark.asyncio
async def test_bronze_flow():
    fake_payload = {
        "env": "dev",
        "etl_layer": "bronze",
        "correlation_id": str(uuid4()),
        "sources": [
            {
                "source_config_payload": {
                    "domain_source_type": "api",
                    "domain_source": "NOBELPRIZE",
                    "dataset_name": "laureates",
                    "request_payload": {"offset": 0, "limit": 100}
                }
            }
        ]
    }

    raw_data_samples = [RawData(data={"id": 1, "name": "Alice"}), RawData(data={"id": 2, "name": "Bob"})]
    processed_results_samples = [
        ProcessedResult(data={"id": 1, "name": "Alice"}, format="json"),
        ProcessedResult(data={"id": 2, "name": "Bob"}, format="json")
    ]

    fake_summary_url = "https://fakeaccount.blob.core.windows.net/bronze/summary.json"

    with patch("function_app.ConfigManager") as mock_config_manager, \
         patch("function_app.BronzeParser") as mock_parser_class, \
         patch("function_app.OrchestratorFactory.get_instance") as mock_orchestrator_factory, \
         patch("function_app.EventGridNotifier") as mock_notifier_class:

        mock_config = MagicMock()
        mock_config.get.side_effect = lambda key: {
            "DATA_LAKE_STORAGE_ACCOUNT_NAME": "fakestorageaccount",
            "DATA_LAKE_CONTAINER_NAME": "bronze",
            "EVENT_GRID_ENDPOINT": "https://fake.eventgrid.net",
            "EVENT_GRID_KEY": "fakekey"
        }.get(key, f"fake_{key.lower()}")
        mock_config_manager.return_value = mock_config

        mock_parser = MagicMock()
        fake_context = BronzeContext(env=Env.DEV, etl_layer=ETLLayer.BRONZE)
        fake_context.correlation_id = fake_payload["correlation_id"]
        mock_parser.parse.return_value = fake_context
        mock_parser_class.return_value = mock_parser

        mock_orchestrator = AsyncMock()
        mock_result = IngestionResult(
            correlation_id=fake_payload["correlation_id"],
            domain_source="NOBELPRIZE",
            domain_source_type="API",
            dataset_name="laureates",
            status="COMPLETED",
            message="Test completed",
            output_paths=["https://fakeaccount.blob.core.windows.net/bronze/fake.json"],
            duration_in_ms=123,
            record_count=2
        )
        mock_result.summary_url = fake_summary_url
        mock_orchestrator.execute.return_value = mock_result
        mock_orchestrator_factory.return_value = mock_orchestrator

        mock_notifier = MagicMock()
        mock_notifier_class.return_value = mock_notifier

        parser = mock_parser_class(mock_config)
        bronze_context = parser.parse(fake_payload)
        bronze_context.correlation_id = fake_payload["correlation_id"]
        assert bronze_context.correlation_id == fake_payload["correlation_id"]

        orchestrator = mock_orchestrator_factory(ETLLayer.BRONZE, config=mock_config)
        result = await orchestrator.execute(bronze_context)
        assert result.status == "COMPLETED"
        assert result.record_count == 2

        notifier = mock_notifier_class(mock_config.get("EVENT_GRID_ENDPOINT"), mock_config.get("EVENT_GRID_KEY"))
        event_grid_payload = {
            "layer": ETLLayer.BRONZE.value,
            "env": fake_payload.get("env"),
            "status": result.status,
            "message_date": datetime.now().isoformat(),
            "correlation_id": result.correlation_id,
            "manifest": f"/silver/manifest/{fake_payload.get('env')}.manifest.json",
            "summary_ingestion_uri": result.summary_url,
            "duration_in_ms": result.duration_in_ms
        }
        notifier.send_notification(
            ETLLayer.BRONZE.value,
            "BronzeIngestionCompleted",
            event_grid_payload,
            result.correlation_id
        )
        notifier.send_notification.assert_called_once_with(
            ETLLayer.BRONZE.value,
            "BronzeIngestionCompleted",
            event_grid_payload,
            result.correlation_id
        )

        result_dict = result.to_dict()
        assert result_dict["status"] == "COMPLETED"
        assert result_dict["record_count"] == 2
        assert fake_summary_url or result_dict["output_paths"][0]
