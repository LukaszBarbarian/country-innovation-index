from ctypes import Structure
from urllib.parse import urlparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import findspark

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

        builder = SparkSession.builder.appName("SparkAzureABFSS").master("local[*]")
        if all_jars:
            builder = builder.config("spark.jars", all_jars)

        builder = builder.config(
            f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", AZURE_STORAGE_ACCOUNT_KEY
        ).config(
            f"spark.hadoop.fs.azure.account.auth.type.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", "SharedKey"
        ).config(
            "spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization", "true"
        )

        self.spark = builder.getOrCreate()
        print("✅ SparkSession gotowa!")

    def https_to_abfss(self, https_url: str) -> str:
        parsed = urlparse(https_url)
        account_name = parsed.netloc.split('.')[0]
        path_parts = parsed.path.lstrip('/').split('/', 1)
        container = path_parts[0]
        blob_path = path_parts[1] if len(path_parts) > 1 else ''
        return f"abfss://{container}@{account_name}.dfs.core.windows.net/{blob_path}"

    def read_json(self, https_url: str, schema: Structure = None) -> DataFrame:
        """
        Odczytuje plik JSON z podanego URL-a. 
        Opcjonalnie przyjmuje schemat dla wydajniejszego parsowania.
        """
        abfss_url = self.https_to_abfss(https_url)
        
        # Jeśli schemat został podany, użyj go. W przeciwnym razie Spark wnioskuje o nim.
        if schema:
            return self.spark.read.schema(schema).json(abfss_url)
        else:
            return self.spark.read.json(abfss_url)

    def write_json(self, df, https_url: str, mode: str = "overwrite"):
        abfss_url = self.https_to_abfss(https_url)
        df.write.mode(mode).json(abfss_url)
