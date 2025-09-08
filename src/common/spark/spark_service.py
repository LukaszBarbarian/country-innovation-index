from ctypes import Structure
from pathlib import Path
import subprocess
import time
from urllib.parse import urlparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from delta import configure_spark_with_delta_pip


from src.common.config.config_manager import ConfigManager


class SparkService:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.spark = None

    def start_local(self):
        AZURE_STORAGE_ACCOUNT_NAME = "demosurdevdatalake4418sa"
        AZURE_STORAGE_ACCOUNT_KEY = self.config.get_setting("AZURE_STORAGE_ACCOUNT_KEY")

        JAR_PATH = "C:/spark/jars"
        all_jars = ",".join([os.path.join(JAR_PATH, jar) for jar in os.listdir(JAR_PATH) if jar.endswith(".jar")])

        builder = SparkSession.builder \
            .appName("SparkAzureABFSS") \
            .master("local[*]") \
            .config("spark.jars", all_jars) \
            .config(f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", AZURE_STORAGE_ACCOUNT_KEY) \
            .config("spark.hadoop.fs.azure.account.auth.type." + AZURE_STORAGE_ACCOUNT_NAME + ".dfs.core.windows.net", "SharedKey") \
            .config("spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()


        self.spark = spark
        print(f"✅ SparkSession gotowa! ver: {spark.version}")



        # try:
        #     print("Spark uruchomiony. Ctrl+C aby zakończyć.")
        #     while True:
        #         time.sleep(60)  # możesz tu też wykonywać jakieś zadania
        # except KeyboardInterrupt:
        #     print("Zamykam Spark...")
        #     spark.stop()        


   
    def https_to_abfss(self, https_url: str) -> str:
        parsed = urlparse(https_url)
        account_name = parsed.netloc.split('.')[0]
        path_parts = parsed.path.lstrip('/').split('/', 1)
        container = path_parts[0].lower() # Zmień na małe litery
        blob_path = path_parts[1] if len(path_parts) > 1 else ''
        return f"abfss://{container}@{account_name}.dfs.core.windows.net/{blob_path}"
    
    def read_csv_https(self, https_url: str, schema: Structure = None) -> DataFrame:
        """
        Odczytuje plik JSON z podanego URL-a. 
        Opcjonalnie przyjmuje schemat dla wydajniejszego parsowania.
        """
        abfss_url = self.https_to_abfss(https_url)
        
        # Jeśli schemat został podany, użyj go. W przeciwnym razie Spark wnioskuje o nim.
        if schema:
            return self.spark.read.option("header", "true").option("inferSchema", "true").schema(schema).csv(abfss_url)
        else:
            return self.spark.read.option("header", "true").csv(abfss_url)    

    def read_json_https(self, https_url: str, schema: Structure = None) -> DataFrame:
        """
        Odczytuje plik JSON z podanego URL-a. 
        Opcjonalnie przyjmuje schemat dla wydajniejszego parsowania.
        """
        abfss_url = self.https_to_abfss(https_url)
        
        # Jeśli schemat został podany, użyj go. W przeciwnym razie Spark wnioskuje o nim.
        if schema:
            return self.spark.read.option("multiline", "true").schema(schema).json(abfss_url)
        else:
            return self.spark.read.json(abfss_url)
        

    def read_delta_abfss(self, abfss_url: str) -> DataFrame:
        """
        Odczytuje dane w formacie Delta z podanego URL-a.
        """
        return self.spark.read.format("delta").load(abfss_url)   


    def read_delta_https(self, https_url: str) -> DataFrame:
        """
        Odczytuje dane w formacie Delta z podanego URL-a.
        """
        abfss_url = self.https_to_abfss(https_url)
        return self.read_delta_abfss(abfss_url)

    def write_json(self, df: DataFrame, abfss_url: str, mode: str = "overwrite", format: str = "delta"):
        df.write.format(format).mode(mode).option("path", abfss_url).save()


    def write_delta(self, df: DataFrame, abfss_url: str, mode: str = "overwrite", partition_cols: list = None, options: dict = None):
        """
        Zapisuje DataFrame jako tabelę Delta, z opcjonalnymi opcjami i partycjonowaniem.
        """
        writer = df.write.format("delta").mode(mode)

        if options:
            for key, value in options.items():
                writer = writer.option(key, value)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(abfss_url)




class DbtRunner:
    def __init__(self, project_dir: str):
        self.project_dir = Path(project_dir)

    def run_models(self, models: str = "all"):
        cmd = f"dbt run --models {models}"
        print(f"🚀 Uruchamiam dbt: {cmd}")
        subprocess.run(cmd, shell=True, cwd=self.project_dir, check=True)
        print("✅ dbt zakończone")                