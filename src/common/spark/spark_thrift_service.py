import subprocess
import os
from pathlib import Path

from src.common.config.config_manager import ConfigManager

# Ważne: Zastąp to bezpiecznym sposobem na pobranie klucza
AZURE_STORAGE_ACCOUNT_NAME = "demosurdevdatalake4418sa"
AZURE_STORAGE_ACCOUNT_KEY = ConfigManager().get("AZURE_STORAGE_ACCOUNT_KEY")


class DbtSparkRunnerSimplified:
    def __init__(self, spark_home: str, port: int = 10000):
        self.spark_home = Path(spark_home)
        self.port = port
        self.process = None

    def start_thrift_server(self):
        spark_submit = self.spark_home / "bin" / "spark-submit.cmd"
        if not spark_submit.exists():
            raise FileNotFoundError(f"Nie znaleziono {spark_submit}")

        delta_warehouse_path = self.spark_home.parent.parent.parent / "data" / "delta_warehouse"
        hive_warehouse_path = self.spark_home.parent.parent.parent / "data" / "hive_warehouse"

        # Lokalne JAR-y (delta-spark 3.0.0)
        delta_jar_path = self.spark_home / "jars" / "delta-spark_2.12-3.0.0.jar"
        if not delta_jar_path.exists():
            raise FileNotFoundError(f"Nie znaleziono pliku JAR Delta Lake: {delta_jar_path}.")

        # Spark submit
        cmd = (
            f'"{spark_submit}" '
            f'--jars "{delta_jar_path}" '
            f'--packages io.delta:delta-storage:3.0.0,org.apache.hadoop:hadoop-azure:3.3.6,com.microsoft.azure:azure-storage:8.6.6 '
            f'--class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 '
            f'--conf spark.hadoop.hadoop.native.io.exception.disable=true '
            f'--master local[*] '
            # Delta Lake konfiguracja
            f'--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
            f'--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
            # Lokalne magazyny
            f'--conf spark.sql.catalog.spark_catalog.warehouse=file:///{delta_warehouse_path} '
            f'--conf spark.sql.warehouse.dir=file:///{hive_warehouse_path} '
            # Azure Data Lake (ABFSS)
            f'--conf spark.hadoop.fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net={AZURE_STORAGE_ACCOUNT_KEY} '
            f'--conf spark.hadoop.fs.azure.account.auth.type.{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net=SharedKey '
            f'--conf spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization=true '
            f'--conf spark.hadoop.fs.abfss.impl=org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem '
            f'--conf spark.hadoop.fs.AbstractFileSystem.abfss.impl=org.apache.hadoop.fs.azurebfs.Abfs '
        )

        print(f"🚀 Uruchamiam Spark Thrift Server na porcie {self.port}...")
        self.process = subprocess.Popen(
            cmd,
            shell=True,
            cwd=self.spark_home,
            env=os.environ
        )
        print("✅ Serwer Spark Thrift powinien teraz działać (uruchomione Popen)")

    def stop_thrift_server(self):
        if self.process:
            self.process.terminate()
            self.process.wait()
            print("✅ Serwer Spark Thrift został zatrzymany.")
