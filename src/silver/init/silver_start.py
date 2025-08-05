import os, sys
import sys
import asyncio
import findspark

sys.path.insert(0, "d:/projects/cv-demo1")
print("sys.path:", sys.path)


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





from src.silver.orchestrators.silver_orchestrator import SilverOrchestrator
from src.silver.contexts.layer_runtime_context import LayerRuntimeContext
from src.silver.contexts.silver_payload_parser import SilverPayloadParser
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
            "api_name": "NOBELPRIZE",
            "dataset_name": "nobelPrizes",
            "layer_name": "bronze",
            "message": "API data successfully processed and stored to Bronze.",
            "api_response_status_code": null,
            "output_path": "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/NOBELPRIZE/2025/08/03/nobelPrizes_736984ab-3b21-4c9f-b2dd-7863f1a74389_c237f2d9.json"
        }
        }
"""


payload = json.loads(json_string)





payload_parser = SilverPayloadParser()
context = payload_parser.parse(payload)

runtime_context = LayerRuntimeContext(spark=spark, layer_context=context)
config = ConfigManager()

orchestrator = SilverOrchestrator(config)

result = asyncio.run(orchestrator.run(runtime_context))



print(result)