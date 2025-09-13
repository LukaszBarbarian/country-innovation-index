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
    """
    A service class for managing a SparkSession and providing utility methods
    for reading and writing data, particularly with Azure Data Lake Storage.
    """
    def __init__(self, config: ConfigManager):
        """
        Initializes the SparkService with a configuration manager.

        Args:
            config (ConfigManager): An instance of ConfigManager for retrieving configuration values.
        """
        self.config = config
        self.spark = None

    def start_local(self):
        """
        Starts a local SparkSession configured to connect to Azure Data Lake Storage
        using Shared Key authentication and Delta Lake support.
        """
        AZURE_STORAGE_ACCOUNT_NAME = "demosurdevdatalake4418sa"
        AZURE_STORAGE_ACCOUNT_KEY = self.config.get("AZURE_STORAGE_ACCOUNT_KEY")

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

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

        print(f"✅ SparkSession is ready! ver: {self.spark.version}")


    def https_to_abfss(self, https_url: str) -> str:
        """
        Converts an HTTPS URL of an Azure Blob Storage resource to an ABFSS URL.

        Args:
            https_url (str): The HTTPS URL of the resource.

        Returns:
            str: The corresponding ABFSS URL.
        """
        parsed = urlparse(https_url)
        account_name = parsed.netloc.split('.')[0]
        path_parts = parsed.path.lstrip('/').split('/', 1)
        container = path_parts[0].lower() # Convert to lowercase
        blob_path = path_parts[1] if len(path_parts) > 1 else ''
        return f"abfss://{container}@{account_name}.dfs.core.windows.net/{blob_path}"
    
    def read_csv_https(self, https_url: str, schema: Structure = None) -> DataFrame:
        """
        Reads a CSV file from a given HTTPS URL.
        Optionally accepts a schema for more efficient parsing.
        
        Args:
            https_url (str): The HTTPS URL of the CSV file.
            schema (Structure): An optional PySpark SQL schema to apply.
            
        Returns:
            DataFrame: A Spark DataFrame containing the data.
        """
        abfss_url = self.https_to_abfss(https_url)
        
        # If a schema is provided, use it. Otherwise, Spark infers it.
        if schema:
            return self.spark.read.option("header", "true").option("inferSchema", "true").schema(schema).csv(abfss_url)
        else:
            return self.spark.read.option("header", "true").csv(abfss_url)    

    def read_json_https(self, https_url: str, schema: Structure = None) -> DataFrame:
        """
        Reads a JSON file from a given HTTPS URL.
        Optionally accepts a schema for more efficient parsing.
        
        Args:
            https_url (str): The HTTPS URL of the JSON file.
            schema (Structure): An optional PySpark SQL schema to apply.
            
        Returns:
            DataFrame: A Spark DataFrame containing the data.
        """
        abfss_url = self.https_to_abfss(https_url)
        
        # If a schema is provided, use it. Otherwise, Spark infers it.
        if schema:
            return self.spark.read.option("multiline", "true").schema(schema).json(abfss_url)
        else:
            return self.spark.read.json(abfss_url)
        
    def read_delta_abfss(self, abfss_url: str) -> DataFrame:
        """
        Reads data in Delta format from a given ABFSS URL.
        
        Args:
            abfss_url (str): The ABFSS URL of the Delta table.
        
        Returns:
            DataFrame: A Spark DataFrame representing the Delta table.
        """
        return self.spark.read.format("delta").load(abfss_url)    

    def read_delta_https(self, https_url: str) -> DataFrame:
        """
        Reads data in Delta format from a given HTTPS URL by converting it to ABFSS.
        
        Args:
            https_url (str): The HTTPS URL of the Delta table.
        
        Returns:
            DataFrame: A Spark DataFrame representing the Delta table.
        """
        abfss_url = self.https_to_abfss(https_url)
        return self.read_delta_abfss(abfss_url)

    def write_json(self, df: DataFrame, abfss_url: str, mode: str = "overwrite", format: str = "delta"):
        """
        Writes a DataFrame to a JSON file.
        This method is marked for potential refactoring as it mentions Delta format
        but is named for JSON.
        """
        df.write.format(format).mode(mode).option("path", abfss_url).save()


    def write_delta(self, df: DataFrame, abfss_url: str, mode: str = "overwrite", partition_cols: list = None, options: dict = None):
        """
        Writes a DataFrame as a Delta table, with optional partitioning and options.
        
        Args:
            df (DataFrame): The Spark DataFrame to write.
            abfss_url (str): The destination ABFSS URL.
            mode (str): The write mode (e.g., "overwrite", "append").
            partition_cols (list): A list of column names to partition the data by.
            options (dict): A dictionary of additional options for the writer.
        """
        writer = df.write.format("delta").mode(mode)

        if options:
            for key, value in options.items():
                writer = writer.option(key, value)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(abfss_url)


class DbtRunner:
    """
    A class for running dbt (data build tool) commands as a subprocess.
    It encapsulates the logic for executing dbt models from a specific project directory.
    """
    def __init__(self, project_dir: str):
        """
        Initializes the DbtRunner.

        Args:
            project_dir (str): The path to the dbt project directory.
        """
        self.project_dir = Path(project_dir)

    def run_models(self, models: str = "all"):
        """
        Executes the 'dbt run' command for specified models.

        Args:
            models (str): A string specifying the models to run (e.g., "all", "my_model").
        """
        cmd = f"dbt run --models {models}"
        print(f"🚀 Running dbt: {cmd}")
        subprocess.run(cmd, shell=True, cwd=self.project_dir, check=True)
        print("✅ dbt finished")