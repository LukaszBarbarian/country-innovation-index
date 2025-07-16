# run_local.py

import os
import sys
import asyncio
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Dodaj główny katalog projektu do ścieżki Python
# Pozwala to na poprawne importowanie modułów
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# --- START: Poprawka dla błędu HADOOP_HOME na Windows ---
# Upewnij się, że ta ścieżka jest poprawna dla Twojego systemu!
# Wstaw tutaj ścieżkę, gdzie skopiowałeś folder winutils/hadoop.
os.environ["HADOOP_HOME"] = "C:/hadoop" # Użyj ukośników w przód!

# Dodatkowo, dodaj folder bin do zmiennej Path
os.environ["PATH"] = f'{os.environ["PATH"]};{os.environ["HADOOP_HOME"]}/bin'
# --- END: Poprawka dla błędu HADOOP_HOME na Windows ---

# Importy Twojego kodu
from src.azure_databricks.common.enums.etl_layers import ETLLayer
from src.azure_databricks.common.enums.env import Env
from src.azure_databricks.utils.local.local_config import LocalConfig
from src.azure_databricks.common.orchestrators.main_orchestrator import MainETLOrchestrator
from src.azure_databricks.utils.local.local_dbutils import LocalDbUtils

# Upewnij się, że te importy są na miejscu, aby dekoratory działały!
import src.azure_databricks.common.orchestrators.register_all_domain_orchestrators
import src.azure_databricks.common.transformers.register_all

def main():
    # Inicjalizacja Spark Session
    builder = SparkSession.builder \
        .appName("LocalETLTest") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "./spark-warehouse")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    local_dbutils = LocalDbUtils()

    local_config = LocalConfig(dbutils_obj=local_dbutils, spark_session=spark)

    main_orchestrator = MainETLOrchestrator(
        spark=spark,
        dbutils_obj=local_dbutils,
        config=local_config,
        env=Env.LOCAL
    )
    
    asyncio.run(main_orchestrator.execute_etl_layer_pipeline(target_etl_layer=ETLLayer.BRONZE))

if __name__ == "__main__":
    main()