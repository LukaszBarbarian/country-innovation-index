from asyncio.log import logger
from re import I
import os, sys
import sys
import asyncio
from injector import V

sys.path.insert(0, "d:/projects/cv-demo1")
print("sys.path:", sys.path)


from src.common.azure_clients.event_grid_client_manager import EventGridClientManager
from src.common.models.orchestrator_result import OrchestratorResult
from src.silver.context.silver_parser import SilverPayloadParser
from src.common.spark.spark_service import SparkService
from src.common.config.config_manager import ConfigManager
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.orchestrator_factory import OrchestratorFactory
import src.silver.init.silver_init 




# df = spark.createDataFrame([
#     (1, "Łukasz"),
#     (2, "Magda"),
#     (3, "Asia")
# ], ["id", "name"])

# print("📤 Zapis do Azure (abfss)...")
# df.write.mode("overwrite").parquet(parquet_path)


from src.common.config.config_manager import ConfigManager


import json

# Ciąg znaków zawierający dane JSON
summary_json = """
		{
    "status": "COMPLETED",
    "env": "dev",
    "etl_layer": "bronze",
    "correlation_id": "b9e88694-8b9b-4430-aab2-cd1e87bc05b3",
    "timestamp": "2025-08-23T09:29:32.874749",
    "processed_items": 6,
    "results": [
        {
            "correlation_id": "b9e88694-8b9b-4430-aab2-cd1e87bc05b3",
            "env": "dev",
            "etl_layer": "bronze",
            "domain_source": "NOBELPRIZE",
            "domain_source_type": "api",
            "dataset_name": "laureates",
            "status": "COMPLETED",
            "message": "API data successfully processed and stored. Uploaded 1 file with 1004 processed results.",
            "output_paths": [
                "nobelprize/2025/08/25/laureates_b9e88694-8b9b-4430-aab2-cd1e87bc05b3_d2b028b2.json"
            ],
            "error_details": {},
            "duration_in_ms": 13650
        },
        {
            "correlation_id": "b9e88694-8b9b-4430-aab2-cd1e87bc05b3",
            "env": "dev",
            "etl_layer": "bronze",
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "population",
            "status": "COMPLETED",
            "message": "API data successfully processed and stored. Uploaded 1 file with 1000 processed results.",
            "output_paths": [
                "worldbank/2025/08/25/population_b9e88694-8b9b-4430-aab2-cd1e87bc05b3_e643b2b9.json"
            ],
            "error_details": {},
            "duration_in_ms": 9406
        },
        {
            "correlation_id": "b9e88694-8b9b-4430-aab2-cd1e87bc05b3",
            "env": "dev",
            "etl_layer": "bronze",
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "pkb",
            "status": "COMPLETED",
            "message": "API data successfully processed and stored. Uploaded 1 file with 1000 processed results.",
            "output_paths": [
                "worldbank/2025/08/25/pkb_b9e88694-8b9b-4430-aab2-cd1e87bc05b3_0cf75120.json"
            ],
            "error_details": {},
            "duration_in_ms": 9071
        },
        {
            "correlation_id": "b9e88694-8b9b-4430-aab2-cd1e87bc05b3",
            "env": "dev",
            "etl_layer": "bronze",
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "rd",
            "status": "COMPLETED",
            "message": "API data successfully processed and stored. Uploaded 1 file with 1000 processed results.",
            "output_paths": [
                "worldbank/2025/08/25/r&d_b9e88694-8b9b-4430-aab2-cd1e87bc05b3_fb7ec9e1.json"
            ],
            "error_details": {},
            "duration_in_ms": 8731
        },
        {
            "correlation_id": "b9e88694-8b9b-4430-aab2-cd1e87bc05b3",
            "env": "dev",
            "etl_layer": "bronze",
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "researchers",
            "status": "COMPLETED",
            "message": "API data successfully processed and stored. Uploaded 1 file with 1000 processed results.",
            "output_paths": [
                "worldbank/2025/08/25/researchers_b9e88694-8b9b-4430-aab2-cd1e87bc05b3_65d57308.json"
            ],
            "error_details": {},
            "duration_in_ms": 9423
        },
        {
            "correlation_id": "b9e88694-8b9b-4430-aab2-cd1e87bc05b3",
            "env": "dev",
            "etl_layer": "bronze",
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "unemployment",
            "status": "COMPLETED",
            "message": "API data successfully processed and stored. Uploaded 1 file with 1000 processed results.",
            "output_paths": [
                "worldbank/2025/08/25/unemployment_b9e88694-8b9b-4430-aab2-cd1e87bc05b3_86116269.json"
            ],
            "error_details": {},
            "duration_in_ms": 8070
        }
    ]
}"""


manifest_json = """
{
  "env": "dev",
  "etl_layer": "silver",
  "references_tables": {
    "boolean_mappings": "/references/boolean_mappings/boolean_mapping.csv",
    "country_codes": "/references/country_codes/country-codes.csv",
    "date_formats": "/references/date_formats/date_formats.csv"
  },
  "manual_data_paths": [
    {
      "domain_source": "PATENTS",
      "dataset_name": "patents",
      "file_path": "/manual/patents/patents.json"
    }
  ],
  "models": [
    {
      "model_name": "COUNTRY",
	    "table_name": "",
      "source_datasets": [
							{
							  "domain_source": "NOBELPRIZE",
							  "dataset_name": "laureates"							  
							},
							{
							  "domain_source": "WORLDBANK",
							  "dataset_name": "population"							  
							}							
						]
    },
    {
      "model_name": "GRADUATE_UNEMPLOYMENT",
	    "table_name": "",
      "source_datasets": [
							{
							  "domain_source": "WORLDBANK",
							  "dataset_name": "unemployment"							  
							}
						 ],
      "depends_on": ["COUNTRY"]
    },
    {
      "model_name": "PKB",
	    "table_name": "",
      "source_datasets": [
							{
							  "domain_source": "WORLDBANK",
							  "dataset_name": "pkb"							  
							}
						 ],
      "depends_on": ["COUNTRY"]
    },{
      "model_name": "RD_EXPENDITURE",
	    "table_name": "",
      "source_datasets": [
							{
							  "domain_source": "WORLDBANK",
							  "dataset_name": "rd"							  
							}
						 ],
      "depends_on": ["COUNTRY"]
    },
    {
      "model_name": "RESEARCHERS",
	    "table_name": "",
      "source_datasets": [
							{
							  "domain_source": "WORLDBANK",
							  "dataset_name": "researchers"							  
							}
						 ],
      "depends_on": ["COUNTRY"]
    }
  ]
}
"""

async def send_event_grid_notification(config: ConfigManager, orchestrator_result: OrchestratorResult):
        """
        Tworzy i wysyła powiadomienie Event Grid na podstawie OrchestratorResult.
        """
        logger.info(f"Sending Event Grid notification for correlation ID: {orchestrator_result.correlation_id}")
        

        event_grid_client = EventGridClientManager(
            endpoint=config.get_setting("EVENT_GRID_ENDPOINT"),
            key=config.get_setting("EVENT_GRID_KEY")
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
            "manifest": "",
            "summary_processing_uri": orchestrator_result.summary_url,
            "duration_in_ms": orchestrator_result.duration_in_ms
        }

        try:
            await event_grid_client.send_event(
                event_type=f"{orchestrator_result.etl_layer.value.capitalize()}IngestionCompleted",
                subject=f"/{orchestrator_result.etl_layer.value}/processing/{orchestrator_result.correlation_id}",
                data=payload
            )
            logger.info("Event Grid notification sent successfully.")
        except Exception as e:
            logger.error(f"Failed to send Event Grid notification: {e}", exc_info=True)
            





config = ConfigManager()
spark = SparkService(config)
spark.start_local()


context = SilverPayloadParser(config).parse(manifest_payload=json.loads(manifest_json), payload=json.loads(summary_json))

orchestrator = OrchestratorFactory.get_instance(ETLLayer.SILVER, spark=spark, config=config)
result = asyncio.run(orchestrator.run(context))
asyncio.run(send_event_grid_notification(config=config, orchestrator_result=result))


