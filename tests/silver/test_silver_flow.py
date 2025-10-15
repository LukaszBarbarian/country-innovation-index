# tests/silver/test_silver_flow.py
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from uuid import uuid4
import json

from src.common.config.config_manager import ConfigManager
from src.silver.context.silver_parser import SilverParser
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.common.azure_clients.event_grid_client_manager import EventGridNotifier
from src.common.spark.spark_service import SparkService
from src.silver.context.silver_context import SilverContext
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.models.orchestrator_result import OrchestratorResult


FAKE_BRONZE_SUMMARY_JSON = """
{
    "status": "COMPLETED",
    "env": "dev",
    "etl_layer": "bronze",
    "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
    "timestamp": "2025-09-17T08:26:58.625145",
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
        "country_codes": "/references/country_codes/country-codes.csv"
    },
    "models": [
        {
            "model_name": "COUNTRY",
            "table_name": "",
            "source_datasets": [
                {"domain_source": "NOBELPRIZE", "dataset_name": "laureates"},
                {"domain_source": "WORLDBANK", "dataset_name": "population"}
            ]
        }
    ]
}"""

@pytest.mark.asyncio
async def test_silver_flow_with_correct_patches():
    fake_correlation_id = "37037a0e-72d8-49d8-93bb-ca9a84b69f5b"
    fake_summary_url = "https://fakeaccount.blob.core.windows.net/silver/summary/37037a0e-72d8-49d8-93bb-ca9a84b69f5b.json"
    fake_processed_items = 12345
    
    bronze_summary_data = json.loads(FAKE_BRONZE_SUMMARY_JSON)
    silver_manifest_data = json.loads(FAKE_SILVER_MANIFEST_JSON)

    with patch("src.common.config.config_manager.ConfigManager") as mock_config_manager, \
         patch("src.silver.context.silver_parser.SilverParser") as mock_parser_class, \
         patch("src.common.factories.orchestrator_factory.OrchestratorFactory") as mock_orchestrator_factory_class, \
         patch("src.common.azure_clients.event_grid_client_manager.EventGridNotifier") as mock_notifier_class, \
         patch("src.common.spark.spark_service.SparkService") as mock_spark_service_class, \
         patch("src.common.models.orchestrator_result.datetime") as mock_datetime:

        fixed_now = datetime(2025, 9, 17, 8, 30, 0)
        mock_datetime.now.return_value = fixed_now

        mock_config = MagicMock(spec=ConfigManager)
        mock_config.get.side_effect = lambda key: {
            "DATA_LAKE_STORAGE_ACCOUNT_NAME": "fakestorageaccount",
            "DATA_LAKE_CONTAINER_NAME": "silver",
            "EVENT_GRID_ENDPOINT": "https://fake.eventgrid.net",
            "EVENT_GRID_KEY": "fakekey",
        }.get(key, f"fake_{key.lower()}")
        mock_config_manager.return_value = mock_config

        mock_spark_service = MagicMock(spec=SparkService)
        mock_spark_service_class.return_value = mock_spark_service
        
        mock_parser = MagicMock(spec=SilverParser)
        fake_context = MagicMock(spec=SilverContext)
        fake_context.env = Env.DEV
        fake_context.etl_layer = ETLLayer.SILVER
        fake_context.correlation_id = fake_correlation_id
        mock_parser.parse.return_value = fake_context
        mock_parser_class.return_value = mock_parser

        mock_orchestrator = AsyncMock()
        mock_result = OrchestratorResult(
            correlation_id=fake_correlation_id,
            env=Env.DEV,
            etl_layer=ETLLayer.SILVER,
            status="COMPLETED",
            message="Silver processing completed successfully",
            summary_url=fake_summary_url,
            duration_in_ms=4567,
            processed_items=fake_processed_items,
            timestamp=fixed_now
        )
        mock_orchestrator.execute.return_value = mock_result
        
        mock_orchestrator_factory_class.get_instance.return_value = mock_orchestrator

        mock_notifier = MagicMock(spec=EventGridNotifier)
        mock_notifier_class.return_value = mock_notifier

        
        spark = mock_spark_service_class(mock_config)
        spark.start_local()
        mock_spark_service.start_local.assert_called_once()
        
        parser = mock_parser_class(mock_config)
        silver_context = parser.parse(silver_manifest_data, bronze_summary_data)
        
        mock_parser.parse.assert_called_once_with(silver_manifest_data, bronze_summary_data)
        assert silver_context.correlation_id == fake_correlation_id
        

        orchestrator = mock_orchestrator_factory_class.get_instance(ETLLayer.SILVER, spark=spark, config=mock_config)
        result = await orchestrator.execute(silver_context)
        
        mock_orchestrator.execute.assert_called_once_with(silver_context)
        assert result.status == "COMPLETED"
        assert result.processed_items == fake_processed_items
        assert result.summary_url == fake_summary_url

        notifier = mock_notifier_class(mock_config.get("EVENT_GRID_ENDPOINT"), mock_config.get("EVENT_GRID_KEY"))
        
        expected_event_grid_payload = {
            "layer": ETLLayer.SILVER.value,
            "env": Env.DEV.value,
            "status": result.status,
            "message_date": result.timestamp.isoformat(),
            "correlation_id": result.correlation_id,
            "manifest": "/gold/manifest/dev.manifest.json",
            "summary_processing_uri": result.summary_url,
            "duration_in_ms": result.duration_in_ms
        }


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

        # 3. Ostateczna weryfikacja
        result_dict = result.to_dict()
        assert result_dict["status"] == "COMPLETED"
        assert result_dict["processed_items"] == fake_processed_items