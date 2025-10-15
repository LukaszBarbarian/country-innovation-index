# tests/silver/test_silver_flow.py
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from uuid import uuid4
import json

from function_app import ETLLayer
from src.silver.context.silver_context import SilverContext
from src.common.enums.env import Env
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.config.config_manager import ConfigManager


FAKE_BRONZE_SUMMARY_JSON = """
{
    "status": "COMPLETED",
    "env": "dev",
    "etl_layer": "bronze",
    "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
    "timestamp": "2020-01-01T10:00:00.000000",
    "processed_items": 6,
    "duration_in_ms": 43330,
    "results": [
        {
            "status": "COMPLETED",
            "domain_source": "NOBELPRIZE",
            "dataset_name": "laureates",
            "output_paths": [
                "abfss://bronze@fakestorageaccount.dfs.core.windows.net/nobelprize/2025/09/17/laureates.json"
            ]
        },
        {
            "status": "COMPLETED",
            "domain_source": "WORLDBANK",
            "dataset_name": "population",
            "output_paths": [
                "abfss://bronze@fakestorageaccount.dfs.core.windows.net/worldbank/2025/09/17/population.json"
            ]
        }
    ]
}"""

FAKE_SILVER_MANIFEST_JSON = """
{
    "env": "dev",
    "etl_layer": "silver",
    "references_tables": {
        "country_codes": "abfss://silver@fakestorageaccount.dfs.core.windows.net/references/country_codes/country-codes.csv"
    },
    "models": [
        {
            "model_name": "COUNTRY",
            "source_datasets": [
                {"domain_source": "NOBELPRIZE", "dataset_name": "laureates"},
                {"domain_source": "WORLDBANK", "dataset_name": "population"}
            ]
        }
    ]
}"""

def side_effect_requests_get(url, *args, **kwargs):
    mock_response = MagicMock()
    if "manifest" in url:
        mock_response.json.return_value = json.loads(FAKE_SILVER_MANIFEST_JSON)
    elif "bronze/summary" in url:
        mock_response.json.return_value = json.loads(FAKE_BRONZE_SUMMARY_JSON)
    else:
        raise ValueError(f"Unexpected URL: {url}")
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    return mock_response

@pytest.mark.asyncio
async def test_silver_flow():
    # 1. Define input to the function (Event Grid payload with link to Bronze Summary)
    fake_input_payload = {
        "env": "dev",
        "etl_layer": "silver",
        "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b", # Use a fixed ID for simplicity
        "bronze_summary_url": "https://fakeaccount.blob.core.windows.net/bronze/summary/37037a0e-72d8-49d8-93bb-ca9a84b69f5b.json",
        "silver_manifest_url": "https://fakeaccount.blob.core.windows.net/silver/manifest/dev.manifest.json"
    }

    # 2. Define expected outputs/mocks
    fake_summary_url = "https://fakeaccount.blob.core.windows.net/silver/summary/37037a0e-72d8-49d8-93bb-ca9a84b69f5b.json"

    # We need to mock the SparkSession, ConfigManager, Parser, Orchestrator, Notifier, and the data fetching (HTTP/Blob)
    with patch("function_app.ConfigManager") as mock_config_manager, \
         patch("function_app.SilverParser") as mock_parser_class, \
         patch("function_app.OrchestratorFactory.get_instance") as mock_orchestrator_factory, \
         patch("function_app.EventGridNotifier") as mock_notifier_class, \
         patch("function_app.requests.get") as mock_requests_get, \
         patch("function_app.SparkService") as mock_spark_service_class:

        # --- Mock ConfigManager ---
        mock_config = MagicMock(spec=ConfigManager)
        mock_config.get.side_effect = lambda key: {
            "DATA_LAKE_STORAGE_ACCOUNT_NAME": "fakestorageaccount",
            "DATA_LAKE_CONTAINER_NAME": "silver",
            "EVENT_GRID_ENDPOINT": "https://fake.eventgrid.net",
            "EVENT_GRID_KEY": "fakekey",
            "SILVER_MANIFEST_FILE_PATH": "/silver/manifest/dev.manifest.json"
        }.get(key, f"fake_{key.lower()}")
        mock_config_manager.return_value = mock_config

        # --- Mock requests.get to simulate fetching manifest and bronze summary ---
        
        
        # NOTE: If your parser reads from Azure Blob Storage directly (not via HTTP), you'd need to mock the Azure client instead of requests.get.
        mock_requests_get.side_effect = side_effect_requests_get

        # --- Mock SparkService (required by Silver Orchestrator and potentially Parser) ---
        mock_spark_service = MagicMock()
        mock_spark_service_class.return_value = mock_spark_service

        # --- Mock SilverParser ---
        mock_parser = MagicMock()
        # Create a mock context object that the parser returns
        fake_context = MagicMock(spec=SilverContext)
        fake_context.env = Env.DEV
        fake_context.etl_layer = ETLLayer.SILVER
        fake_context.correlation_id = fake_input_payload["correlation_id"]
        # Ensure the parser is called with the manifest and summary JSON data
        mock_parser.parse.return_value = fake_context
        mock_parser_class.return_value = mock_parser


        # --- Mock Orchestrator ---
        mock_orchestrator = AsyncMock()
        mock_result = OrchestratorResult(
            correlation_id=fake_input_payload["correlation_id"],
            env=Env.DEV,
            etl_layer=ETLLayer.SILVER,
            status="COMPLETED",
            message="Silver processing completed successfully",
            summary_url=fake_summary_url,
            duration_in_ms=4567,
            processed_record_count=15000, # A realistic mock count
            error_details={}
        )
        mock_orchestrator.execute.return_value = mock_result
        mock_orchestrator_factory.return_value = mock_orchestrator

        # --- Mock EventGridNotifier ---
        mock_notifier = MagicMock()
        mock_notifier_class.return_value = mock_notifier

        # =========================================================================
        # 3. Simulate the flow (steps from your silver_start and bronze_test)
        # =========================================================================
        
        # Initialize Spark (if needed by your Silver Flow logic)
        spark = mock_spark_service_class(mock_config)
        spark.start_local.assert_called_once() # Verify Spark is started

        # 1. Parse the input (The parser logic might involve fetching the JSONs)
        parser = mock_parser_class(mock_config)
        # We need to simulate how your SilverParser gets the manifest and summary
        # Based on your silver_start: context = SilverParser(config).parse(manifest_json, summary_json)
        # So we mock the internal call to fetch the data first, and then call parse with the data.
        # Here we directly pass the fake data to the mock parser's 'parse' method call:
        bronze_summary_data = json.loads(FAKE_BRONZE_SUMMARY_JSON)
        silver_manifest_data = json.loads(FAKE_SILVER_MANIFEST_JSON)
        
        # NOTE: Depending on your 'function_app' entry point, the parsing might be different.
        # This simulates the logic from your `silver_start.py` file:
        silver_context = parser.parse(silver_manifest_data, bronze_summary_data)
        
        mock_parser.parse.assert_called_once_with(silver_manifest_data, bronze_summary_data)
        assert silver_context.correlation_id == fake_input_payload["correlation_id"]

        # 2. Execute the Orchestrator
        orchestrator = mock_orchestrator_factory(ETLLayer.SILVER, config=mock_config, spark=spark)
        result = await orchestrator.execute(silver_context)
        
        mock_orchestrator.execute.assert_called_once_with(silver_context)
        assert result.status == "COMPLETED"
        assert result.processed_record_count == 15000

        # 3. Send Notification
        notifier = mock_notifier_class(mock_config.get("EVENT_GRID_ENDPOINT"), mock_config.get("EVENT_GRID_KEY"))
        
        # Define the expected Event Grid payload based on your send_event_grid_notification function
        expected_event_grid_payload = {
            "layer": ETLLayer.SILVER.value,
            "env": Env.DEV.value,
            "status": result.status,
            # We mock the timestamp for deterministic testing; a real test might mock datetime.now()
            "message_date": result.timestamp.isoformat(), 
            "correlation_id": result.correlation_id,
            "manifest": "/gold/manifest/dev.manifest.json",
            "summary_processing_uri": result.summary_url,
            "duration_in_ms": result.duration_in_ms
        }
        
        # We need to update the mock result's timestamp to match the expected payload in the assert
        # A more robust approach would be to mock datetime.now() before OrchestratorResult is created
        mock_result.timestamp = datetime.strptime(
            "2020-01-01T10:00:00.000000", "%Y-%m-%dT%H:%M:%S.%f"
        )
        expected_event_grid_payload["message_date"] = mock_result.timestamp.isoformat()


        notifier.send_notification(
            ETLLayer.SILVER.value,
            "SilverProcessCompleted",
            expected_event_grid_payload,
            result.correlation_id
        )

        notifier.send_notification.assert_called_once_with(
            ETLLayer.SILVER.value,
            "SilverProcessCompleted",
            expected_event_grid_payload,
            result.correlation_id
        )
        
        # 4. Final assertions
        result_dict = result.to_dict()
        assert result_dict["status"] == "COMPLETED"
        assert result_dict["processed_record_count"] == 15000
        assert result_dict["summary_url"] == fake_summary_url