# main.py
import asyncio
from asyncio.log import logger
import json
import sys



sys.path.insert(0, "d:/projects/cv-demo1")
print("sys.path:", sys.path)

from src.gold.contexts.gold_parser import GoldParser
from src.common.azure_clients.event_grid_client_manager import EventGridClientManager
from src.common.models.orchestrator_result import OrchestratorResult
from src.common.enums.etl_layers import ETLLayer
from src.common.config.config_manager import ConfigManager
from src.common.spark.spark_service import SparkService
from src.common.factories.orchestrator_factory import OrchestratorFactory
import src.gold.init.gold_init


def send_event_grid_notification(config: ConfigManager, orchestrator_result: OrchestratorResult):
        """
        Tworzy i wysyła powiadomienie Event Grid na podstawie OrchestratorResult.
        """
        logger.info(f"Sending Event Grid notification for correlation ID: {orchestrator_result.correlation_id}")
        

        event_grid_client = EventGridClientManager(
            endpoint=config.get("EVENT_GRID_ENDPOINT"),
            key=config.get("EVENT_GRID_KEY")
        )
        if not event_grid_client:
            logger.error("Event Grid client not initialized. Cannot send event.")
            return

        payload = {
            "layer": orchestrator_result.etl_layer.value,
            "env": orchestrator_result.env.value,
            "status": orchestrator_result.status,
            "message_date": orchestrator_result.timestamp.isoformat(),
            "correlation_id": orchestrator_result.correlation_id,
            "manifest": "/gold/manifest/dev.manifest.json",
            "summary_processing_uri": orchestrator_result.summary_url,
            "duration_in_ms": orchestrator_result.duration_in_ms
        }

        try:
            event_grid_client.send_event(
                event_type=f"{orchestrator_result.etl_layer.value.capitalize()}ProcessCompleted",
                subject=f"/{orchestrator_result.etl_layer.value}/processing/{orchestrator_result.correlation_id}",
                data=payload
            )
            logger.info("Event Grid notification sent successfully.")
        except Exception as e:
            logger.error(f"Failed to send Event Grid notification: {e}", exc_info=True)
            




manifest_json = """
{
  "env": "dev",
  "etl_layer": "gold",
  "dims": [
    {
      "name": "dim_country",
      "source_models": ["COUNTRY"],
      "primary_keys": ["country_code"]
    },
    {
      "name": "dim_year",
      "source_models": ["YEAR"],
      "primary_keys": ["year"]
    }
  ],
  "facts": [
    {
      "name": "fact_innovation",
      "source_models": ["COUNTRY", "POPULATION", "GRADUATE_UNEMPLOYMENT", "RESEARCHERS", "PKB", "RD_EXPENDITURE", "PATENTS", "NOBEL_LAUREATES", "YEAR"],
      "primary_keys": ["country_code"]
    }
  ]
}
"""


summary_json = """
{
    "status": "COMPLETED",
    "env": "dev",
    "etl_layer": "silver",
    "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
    "timestamp": "2025-09-17T09:29:18.173977",
    "processed_models": 9,
    "duration_in_ms": 651754,
    "results": [
        {
            "model": "COUNTRY",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/COUNTRY",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 249,
            "error_details": {},
            "duration_in_ms": 39817
        },
        {
            "model": "YEAR",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/YEAR",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 125,
            "error_details": {},
            "duration_in_ms": 10295
        },
        {
            "model": "NOBEL_LAUREATES",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/NOBEL_LAUREATES",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 428,
            "error_details": {},
            "duration_in_ms": 13443
        },
        {
            "model": "GRADUATE_UNEMPLOYMENT",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/GRADUATE_UNEMPLOYMENT",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 13975,
            "error_details": {},
            "duration_in_ms": 31691
        },
        {
            "model": "PKB",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/PKB",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 13975,
            "error_details": {},
            "duration_in_ms": 61107
        },
        {
            "model": "RD_EXPENDITURE",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/RD_EXPENDITURE",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 13975,
            "error_details": {},
            "duration_in_ms": 86466
        },
        {
            "model": "RESEARCHERS",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/RESEARCHERS",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 13975,
            "error_details": {},
            "duration_in_ms": 106308
        },
        {
            "model": "PATENTS",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/PATENTS",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 5132,
            "error_details": {},
            "duration_in_ms": 144432
        },
        {
            "model": "POPULATION",
            "status": "COMPLETED",
            "output_path": "abfss://silver@demosurdevdatalake4418sa.dfs.core.windows.net/POPULATION",
            "correlation_id": "37037a0e-72d8-49d8-93bb-ca9a84b69f5b",
            "operation_type": "UPSERT",
            "record_count": 13975,
            "error_details": {},
            "duration_in_ms": 158195
        }
    ]
}
"""





config = ConfigManager()
context = GoldParser().parse(manifest_json=manifest_json, summary_json=summary_json)

spark = SparkService(config)
spark.start_local()


orchestrator = OrchestratorFactory.get_instance(ETLLayer.GOLD, spark=spark, config=config)
result = asyncio.run(orchestrator.execute(context))
send_event_grid_notification(config=config, orchestrator_result=result)