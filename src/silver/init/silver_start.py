import os, sys
import sys
import asyncio
import findspark

from src.common.config.references_config_parser import ReferenceConfigParser
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.orchestrator_factory import OrchestratorFactory
from src.silver.context.silver_parser import SilverPayloadParser



sys.path.insert(0, "d:/projects/cv-demo1")
print("sys.path:", sys.path)

import src.silver.init.silver_init 
from src.common.config.config_manager import ConfigManager

SPARK_INSTALL_PATH = "C:/spark/spark-3.5.6-bin-hadoop3"
findspark.init(SPARK_INSTALL_PATH)


config = ConfigManager()
AZURE_STORAGE_ACCOUNT_NAME = "demosurdevdatalake4418sa"
AZURE_STORAGE_ACCOUNT_KEY = config.get_setting("AZURE_STORAGE_ACCOUNT_KEY")#os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_CONTAINER_NAME = "bronze"


parquet_path = f"abfss://{AZURE_CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
JAR_PATH = "C:/spark/jars"
all_jars = ",".join([
    os.path.join(JAR_PATH, jar)
    for jar in os.listdir(JAR_PATH) if jar.endswith(".jar")
])

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkAzureABFSS") \
    .master("local[*]") \
    .config("spark.jars", all_jars) \
    .config(f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", AZURE_STORAGE_ACCOUNT_KEY) \
    .config("spark.hadoop.fs.azure.account.auth.type." + AZURE_STORAGE_ACCOUNT_NAME + ".dfs.core.windows.net", "SharedKey") \
    .config("spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization", "true") \
    .getOrCreate()

print("✅ SparkSession gotowa!")

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
json_string = """
{
        "correlation_id": "736984ab-3b21-4c9f-b2dd-7863f1a74389",
        "queue_message_id": "209a080e-c116-49c8-9fe8-393555906fb1",
        "env": "dev",
        "etl_layer": "bronze",
        "processing_config_payload": {
                "status": "COMPLETED",
                "correlation_id": "abc-123",
                "queue_message_id": "xyz-789",
                "domain_source": "PATENTS",
                "domain_source_type": "static_file",
                "dataset_name": "patents",
                "layer_name": "bronze",
                "env": "dev",
                "message": "Static file path passed through to the next layer.",
                "source_response_status_code": 200,
                "output_paths": [
                    "/manual/patents/patents.json"
                ]
            }
        }
"""


payload = json.loads(json_string)


json_string = """
{
  "manual_data_paths": [
    {
      "domain_source": "PATENTS",
      "dataset_name": "patents",
      "file_path": "/manual/patents/patents.json"
    }
  ],
  "references": {
    "country_codes": "/references/country_codes.csv"
  }
}
"""

config = json.loads(json_string)

reference = ReferenceConfigParser().parse(config)
context = SilverPayloadParser().parse(payload)

config = ConfigManager(reference)
orchestrator = OrchestratorFactory.get_instance(ETLLayer.SILVER, spark=spark, config=config)
result = asyncio.run(orchestrator.run(context))



print(result)