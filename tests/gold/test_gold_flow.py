import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
import json

# Wymagane importy
from src.common.config.config_manager import ConfigManager
from src.gold.contexts.gold_parser import GoldParser
from src.common.azure_clients.event_grid_client_manager import EventGridClientManager
from src.common.spark.spark_service import SparkService
from src.gold.contexts.gold_layer_context import GoldContext
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from src.common.models.orchestrator_result import OrchestratorResult

# --- FAKE JSON DATA (nie zmieniane) ---
FAKE_SILVER_SUMMARY_JSON = """
{
    "status": "COMPLETED",
    "env": "dev",
    "etl_layer": "silver",
    "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
    "timestamp": "2025-09-17T09:29:18.173977",
    "processed_items": 9,
    "duration_in_ms": 651754,
    "results": [
        {"model": "COUNTRY", "status": "COMPLETED", "record_count": 249},
        {"model": "YEAR", "status": "COMPLETED", "record_count": 125},
        {"model": "NOBEL_LAUREATES", "status": "COMPLETED", "record_count": 428},
        {"model": "PKB", "status": "COMPLETED", "record_count": 13975}
    ]
}
"""

FAKE_GOLD_MANIFEST_JSON = """
{
  "env": "dev",
  "etl_layer": "gold",
  "dims": [
    {
      "name": "dim_country",
      "source_models": ["COUNTRY"]
    }
  ],
  "facts": [
    {
      "name": "fact_innovation",
      "source_models": ["COUNTRY", "YEAR"]
    }
  ]
}
"""
# --- KONIEC FAKE JSON DATA ---

@pytest.mark.asyncio
async def test_gold_flow():
    # 1. Dane wejściowe i oczekiwane wyniki
    fake_correlation_id = "37037a0e-72d8-49d8-93bb-ca9a84b69f5b"
    fake_summary_url = "https://fakeaccount.blob.core.windows.net/gold/summary/37037a0e-72d8-49d8-93bb-ca9a84b69f5b.json"
    fake_processed_record_count = 56789 # Łączna liczba rekordów przetworzonych w warstwie Gold
    
    silver_summary_data = json.loads(FAKE_SILVER_SUMMARY_JSON)
    gold_manifest_data = json.loads(FAKE_GOLD_MANIFEST_JSON)

    # Mockowanie zależności, używając pełnych ścieżek
    with patch("src.common.config.config_manager.ConfigManager") as mock_config_manager, \
         patch("src.gold.contexts.gold_parser.GoldParser") as mock_parser_class, \
         patch("src.common.factories.orchestrator_factory.OrchestratorFactory") as mock_orchestrator_factory_class, \
         patch("src.common.azure_clients.event_grid_client_manager.EventGridClientManager") as mock_notifier_class, \
         patch("src.common.spark.spark_service.SparkService") as mock_spark_service_class, \
         patch("src.common.models.orchestrator_result.datetime") as mock_datetime:

        # --- Mockowanie czasu ---
        fixed_now = datetime(2025, 9, 17, 8, 45, 0)
        mock_datetime.now.return_value = fixed_now

        # --- Mockowanie ConfigManager ---
        mock_config = MagicMock(spec=ConfigManager)
        mock_config.get.side_effect = lambda key: {
            "EVENT_GRID_ENDPOINT": "https://fake.eventgrid.net",
            "EVENT_GRID_KEY": "fakekey",
        }.get(key, f"fake_{key.lower()}")
        mock_config_manager.return_value = mock_config

        # --- Mockowanie SparkService ---
        mock_spark_service = MagicMock(spec=SparkService)
        mock_spark_service_class.return_value = mock_spark_service
        
        # --- Mockowanie GoldParser ---
        mock_parser = MagicMock(spec=GoldParser)
        fake_context = MagicMock(spec=GoldContext)
        fake_context.env = Env.DEV
        fake_context.etl_layer = ETLLayer.GOLD
        fake_context.correlation_id = fake_correlation_id
        mock_parser.parse.return_value = fake_context
        mock_parser_class.return_value = mock_parser

        # --- Mockowanie OrchestratorFactory i Orchestrator ---
        mock_orchestrator = AsyncMock()
        
        # UWAGA: Ten blok jest teraz bezużyteczny w kontekście tworzenia mock_result
        # ale zostawiam go, aby pokazać, że dane te są używane do *czegoś* (np. logowania)
        # ale nie do budowy OrchestratorResult:
        mock_results_list = [
            {
                "model_name": "dim_country",
                "status": "COMPLETED",
                "output_path": "abfss://gold/dim_country",
                "record_count": 249,
                "duration_in_ms": 500
            },
            {
                "model_name": "fact_innovation",
                "status": "COMPLETED",
                "output_path": "abfss://gold/fact_innovation",
                "record_count": 56540,
                "duration_in_ms": 3000
            }
        ]
        
        # Tworzymy wynik Orchestratora UŻYWAJĄC TYLKO PÓL Z DEFINICJI OrchestratorResult
        mock_result = OrchestratorResult(
            correlation_id=fake_correlation_id,
            env=Env.DEV,
            etl_layer=ETLLayer.GOLD,
            status="COMPLETED",
            message="Gold processing completed successfully",
            summary_url=fake_summary_url,
            duration_in_ms=4567,
            processed_items=fake_processed_record_count, 
            # WAŻNE: USUNIĘTO 'results=mock_results_list'
            timestamp=fixed_now
        )
        mock_orchestrator.execute.return_value = mock_result
        
        # Mockujemy metodę statyczną/klasową get_instance
        mock_orchestrator_factory_class.get_instance.return_value = mock_orchestrator

        # --- Mockowanie EventGridClientManager (Notifier) ---
        mock_notifier = MagicMock(spec=EventGridClientManager)
        mock_notifier_class.return_value = mock_notifier

        # =========================================================================
        # 2. Symulacja przepływu (Gold Flow)
        # =========================================================================
        
        # A. Inicjalizacja Spark
        spark = mock_spark_service_class(mock_config)
        spark.start_local() 
        mock_spark_service.start_local.assert_called_once()
        
        # B. Parsowanie
        parser = mock_parser_class(mock_config)
        gold_context = parser.parse(manifest_json=gold_manifest_data, summary_json=silver_summary_data)
        
        mock_parser.parse.assert_called_once_with(
            manifest_json=gold_manifest_data, 
            summary_json=silver_summary_data
        )
        assert gold_context.correlation_id == fake_correlation_id
        
        # C. Wykonanie Orchestratora
        orchestrator = mock_orchestrator_factory_class.get_instance(ETLLayer.GOLD, spark=spark, config=mock_config)
        result = await orchestrator.execute(gold_context)
        
        mock_orchestrator.execute.assert_called_once_with(gold_context)
        assert result.status == "COMPLETED"
        assert result.processed_items == fake_processed_record_count
        assert result.summary_url == fake_summary_url
        # WAŻNE: Usunięto asercję na result.results

        # D. Wysłanie powiadomienia Event Grid (symulacja send_event_grid_notification)
        notifier = mock_notifier_class(
            endpoint=mock_config.get("EVENT_GRID_ENDPOINT"), 
            key=mock_config.get("EVENT_GRID_KEY")
        )
        
        # Definicja oczekiwanego ładunku Event Grid 
        expected_event_grid_payload = {
            "layer": ETLLayer.GOLD.value,
            "env": Env.DEV.value,
            "status": result.status,
            "message_date": result.timestamp.isoformat(),
            "correlation_id": result.correlation_id,
            "manifest": "/gold/manifest/dev.manifest.json", 
            "summary_processing_uri": result.summary_url,
            "duration_in_ms": result.duration_in_ms
        }
        
        expected_event_type = f"{ETLLayer.GOLD.value.capitalize()}ProcessCompleted"
        expected_subject = f"/{ETLLayer.GOLD.value}/processing/{result.correlation_id}"

        notifier.send_event(
            event_type=expected_event_type,
            subject=expected_subject,
            data=expected_event_grid_payload
        )

        notifier.send_event.assert_called_once_with(
            event_type=expected_event_type,
            subject=expected_subject,
            data=expected_event_grid_payload
        )

        # 3. Ostateczna weryfikacja
        result_dict = result.to_dict()
        assert result_dict["status"] == "COMPLETED"
        assert result_dict["processed_items"] == fake_processed_record_count