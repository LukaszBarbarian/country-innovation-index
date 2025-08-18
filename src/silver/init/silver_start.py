import os, sys
import sys
import asyncio
from injector import V

from src.silver.context.silver_parser import SilverPayloadParser


sys.path.insert(0, "d:/projects/cv-demo1")
print("sys.path:", sys.path)



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
summary_json = """{
		"status": "BRONZE_COMPLETED",
		"env": "dev",
		"layer_name": "bronze",
		"correlation_id": "011d2629-dae2-42ac-92b3-861d94c98462",
		"timestamp": "2025-08-18T07:50:19.256179Z",
		"processed_items": 1,
		"results": [
			{
				"correlation_id": "011d2629-dae2-42ac-92b3-861d94c98462",
				"env": "dev",
				"etl_layer": "bronze",
				"domain_source": "NOBELPRIZE",
				"domain_source_type": "api",
				"dataset_name": "laureates",
				"status": "SKIPPED",
				"message": "No new files uploaded.",
				"output_paths": [],
				"error_details": {},
				"duration_in_ms": 0
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
				"model_name": "country",
				"source_datasets": [
					"laureates",
					"patents"
				],
				"status": "pending",
				"errors": []
			}
		]
	}
"""

context = SilverPayloadParser().parse(manifest_payload=json.loads(manifest_json), payload=json.loads(summary_json))

config = ConfigManager()
spark = SparkService(config)
spark.start_local()

orchestrator = OrchestratorFactory.get_instance(ETLLayer.SILVER, spark=spark, config=config)
result = asyncio.run(orchestrator.run(context))



print(result)