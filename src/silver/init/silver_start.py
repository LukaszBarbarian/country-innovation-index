import os, sys
import sys
import asyncio
from injector import V

sys.path.insert(0, "d:/projects/cv-demo1")
print("sys.path:", sys.path)



from src.common.spark.spark_service import SparkService
from src.common.config.config_manager import ConfigManager
from src.common.config.references_config_parser import ReferenceConfigParser
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.silver.context.silver_parser import SilverPayloadParser
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
payload = {"status": "BRONZE_COMPLETED", "env" : "dev", "correlation_id": "a104eb07-180d-4f0b-8e52-2007edc73ffc", "timestamp": "2025-08-10T14:42:54.996192+00:00", "processed_items": 2, 
           "results":
                    [
                        {"status": "COMPLETED", "correlation_id": "63f5fa96-e202-4446-84cc-a43dbe337160", "layer_name": "bronze", "env": "dev", "message": "API data successfully processed and stored. Uploaded 1 files.", "domain_source": "NOBELPRIZE", "domain_source_type": "api", "dataset_name": "laureates", "output_paths": ["https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/NOBELPRIZE/2025/08/13/laureates_d9cc180b-14a5-4c39-94b2-ce8ff0783c4a_7122d7e2.json"], "error_details": {}}, 
                        {"status": "COMPLETED", "correlation_id": "63f5fa96-e202-4446-84cc-a43dbe337160", "layer_name": "bronze", "env": "dev", "message": "Static file path passed through to the next layer.", "domain_source": "PATENTS", "domain_source_type": "static_file", "dataset_name": "patents", "output_paths": ["https://demosurdevdatalake4418sa.blob.core.windows.net/manual/patents/patents.json"], "error_details": {}}
                    ]
          }



json_string = """
{
  "references": {
    "country_codes": "/references/country_codes.csv"
  }
}
"""

config = json.loads(json_string)

reference = ReferenceConfigParser().parse(config)
context = SilverPayloadParser().parse(payload)

config = ConfigManager(reference)
spark = SparkService(config)
spark.start_local()

orchestrator = OrchestratorFactory.get_instance(ETLLayer.SILVER, spark=spark, config=config)
result = asyncio.run(orchestrator.run(context))



print(result)