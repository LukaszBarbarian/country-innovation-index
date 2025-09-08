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
from src.silver.context.silver_parser import SilverParser
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
    "correlation_id": "56f0510e-47c5-4f42-9fa8-7d3d65254e2b",
    "timestamp": "2025-09-03T16:29:05.373451",
    "processed_items": 6,
    "duration_in_ms": 111100,
    "results": [
        {
            "status": "COMPLETED",
            "correlation_id": "56f0510e-47c5-4f42-9fa8-7d3d65254e2b",
            "etl_layer": "bronze",
            "env": "dev",
            "duration_in_ms": 6901,
            "record_count": 1004,
            "domain_source": "NOBELPRIZE",
            "domain_source_type": "api",
            "dataset_name": "laureates",
            "message": "API data successfully processed. Uploaded 1 file with 1004 records.",
            "output_paths": [
                "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/nobelprize/2025/09/03/laureates_56f0510e-47c5-4f42-9fa8-7d3d65254e2b_d2b028b2.json"
            ],
            "error_details": {}
        },
        {
            "status": "COMPLETED",
            "correlation_id": "56f0510e-47c5-4f42-9fa8-7d3d65254e2b",
            "etl_layer": "bronze",
            "env": "dev",
            "duration_in_ms": 19600,
            "record_count": 17290,
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "population",
            "message": "API data successfully processed. Uploaded 1 file with 17290 records.",
            "output_paths": [
                "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/worldbank/2025/09/03/population_56f0510e-47c5-4f42-9fa8-7d3d65254e2b_e643b2b9.json"
            ],
            "error_details": {}
        },
        {
            "status": "COMPLETED",
            "correlation_id": "56f0510e-47c5-4f42-9fa8-7d3d65254e2b",
            "etl_layer": "bronze",
            "env": "dev",
            "duration_in_ms": 26026,
            "record_count": 17290,
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "pkb",
            "message": "API data successfully processed. Uploaded 1 file with 17290 records.",
            "output_paths": [
                "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/worldbank/2025/09/03/pkb_56f0510e-47c5-4f42-9fa8-7d3d65254e2b_0cf75120.json"
            ],
            "error_details": {}
        },
        {
            "status": "COMPLETED",
            "correlation_id": "56f0510e-47c5-4f42-9fa8-7d3d65254e2b",
            "etl_layer": "bronze",
            "env": "dev",
            "duration_in_ms": 19272,
            "record_count": 17290,
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "rd",
            "message": "API data successfully processed. Uploaded 1 file with 17290 records.",
            "output_paths": [
                "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/worldbank/2025/09/03/rd_56f0510e-47c5-4f42-9fa8-7d3d65254e2b_fb7ec9e1.json"
            ],
            "error_details": {}
        },
        {
            "status": "COMPLETED",
            "correlation_id": "56f0510e-47c5-4f42-9fa8-7d3d65254e2b",
            "etl_layer": "bronze",
            "env": "dev",
            "duration_in_ms": 18925,
            "record_count": 17290,
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "researchers",
            "message": "API data successfully processed. Uploaded 1 file with 17290 records.",
            "output_paths": [
                "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/worldbank/2025/09/03/researchers_56f0510e-47c5-4f42-9fa8-7d3d65254e2b_65d57308.json"
            ],
            "error_details": {}
        },
        {
            "status": "COMPLETED",
            "correlation_id": "56f0510e-47c5-4f42-9fa8-7d3d65254e2b",
            "etl_layer": "bronze",
            "env": "dev",
            "duration_in_ms": 20376,
            "record_count": 17290,
            "domain_source": "WORLDBANK",
            "domain_source_type": "api",
            "dataset_name": "unemployment",
            "message": "API data successfully processed. Uploaded 1 file with 17290 records.",
            "output_paths": [
                "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/worldbank/2025/09/03/unemployment_56f0510e-47c5-4f42-9fa8-7d3d65254e2b_86116269.json"
            ],
            "error_details": {}
        }
    ]
}"""


manifest_json = """
{
  "env": "dev",
  "etl_layer": "silver",
  "references_tables": {
    "country_codes": "/references/country_codes/country-codes.csv"
  },
  "manual_data_paths": [
    {
      "domain_source": "PATENTS",
      "dataset_name": "patents",
      "file_path": "/manual/patents/patents.csv"
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
							},
              {
							  "domain_source": "PATENTS",
							  "dataset_name": "patents"							  
							}		
						]
    },
    {
      "model_name": "YEAR",
	    "table_name": "",
      "source_datasets": []
    },
    {
      "model_name": "NOBEL_LAUREATES",
	    "table_name": "",
      "source_datasets": [
							{
							  "domain_source": "NOBELPRIZE",
							  "dataset_name": "laureates"							  
							}
						 ],
      "depends_on": ["COUNTRY"]
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
    },
    {
      "model_name": "PATENTS",
	    "table_name": "",
      "source_datasets": [
							{
							  "domain_source": "PATENTS",
							  "dataset_name": "patents"							  
							}
						 ],
      "depends_on": ["COUNTRY"]
    },
    {
      "model_name": "POPULATION",
	    "table_name": "",
      "source_datasets": [
							{
							  "domain_source": "WORLDBANK",
							  "dataset_name": "population"							  
							}
						 ],
      "depends_on": ["COUNTRY"]
    }
  ]
}
"""

def send_event_grid_notification(config: ConfigManager, orchestrator_result: OrchestratorResult):
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
            





config = ConfigManager()
config.get("AZURE_WEB_JOBS_STORAGE")
# spark = SparkService(config)
# spark.start_local()


# context = SilverParser().parse(manifest_json, summary_json)

# orchestrator = OrchestratorFactory.get_instance(ETLLayer.SILVER, spark=spark, config=config)
# result = asyncio.run(orchestrator.execute(context))
# send_event_grid_notification(config=config, orchestrator_result=result)


