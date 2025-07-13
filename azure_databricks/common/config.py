from dataclasses import dataclass

# Usunięcie importu dbutils stąd!
# import dbutils # <-- USUŃ TĘ LINIĘ

class Secrets:
    @classmethod
    # dbutils musi być przekazane!
    def get_secret(cls, dbutils_obj, secret_scope: str, secret_key: str):
        """Pobiera sekret z podanego zakresu i klucza."""
        try:
            return dbutils_obj.secrets.get(scope=secret_scope, key=secret_key)
        except Exception as e:
            raise ValueError(f"Nie udało się pobrać sekretu '{secret_key}' z zakresu '{secret_scope}'. Sprawdź uprawnienia i istnienie sekretu. Błąd: {e}")


@dataclass
class Config:
    # Dodaj dbutils_obj i spark_session jako argumenty konstruktora
    dbutils_obj: any
    spark_session: any # Typowanie jako 'any' dla prostoty, możesz użyć 'pyspark.sql.SparkSession'

    DATABRICKS_SECRET_SCOPE: str = "demosur_dev_secret_scope"
    DATABRICKS_ACCESS_CONNECTOR_ID_KEY: str = "databricks-access-connector-id"
    DATALAKE_STORAGE_ACCOUNT_NAME_KEY: str = "datalake-storage-account-name"

    _BRONZE_CONTAINER_PREFIX: str = "bronze"
    _SILVER_CONTAINER_PREFIX: str = "silver"
    _GOLD_CONTAINER_PREFIX: str = "gold"

    @property
    def datalake_storage_account_name(self) -> str:
        """Pobiera dynamiczną nazwę konta storage z sekretów."""
        # Użyj przekazanego dbutils_obj
        return Secrets.get_secret(self.dbutils_obj, self.DATABRICKS_SECRET_SCOPE, self.DATALAKE_STORAGE_ACCOUNT_NAME_KEY)

    @property
    def databricks_access_connector_id(self) -> str:
        """Pobiera ID konektora dostępowego z sekretów."""
        # Użyj przekazanego dbutils_obj
        return Secrets.get_secret(self.dbutils_obj, self.DATABRICKS_SECRET_SCOPE, self.DATABRICKS_ACCESS_CONNECTOR_ID_KEY)

    @property
    def BRONZE_CONTAINER(self) -> str:
        """Zwraca pełną ścieżkę do kontenera Bronze."""
        return f"abfss://{self._BRONZE_CONTAINER_PREFIX}@{self.datalake_storage_account_name}.dfs.core.windows.net/"

    @property
    def SILVER_CONTAINER(self) -> str:
        """Zwraca pełną ścieżkę do kontenera Silver."""
        return f"abfss://{self._SILVER_CONTAINER_PREFIX}@{self.datalake_storage_account_name}.dfs.core.windows.net/"

    @property
    def GOLD_CONTAINER(self) -> str:
        """Zwraca pełną ścieżkę do kontenera Gold."""
        return f"abfss://{self._GOLD_CONTAINER_PREFIX}@{self.datalake_storage_account_name}.dfs.core.windows.net/"

    def configure_spark_for_adls_gen2(self): # Usunięto 'spark' jako argument, używamy spark_session z klasy
        """
        Konfiguruje Sparka do dostępu do ADLS Gen2 za pomocą Service Principal/Access Connector.
        """
        # Użyj przekazanego spark_session
        self.spark_session.conf.set("fs.azure.account.auth.type", "OAuth")
        self.spark_session.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark_session.conf.set("fs.azure.account.oauth2.client.id", self.databricks_access_connector_id)
        # Pamiętaj, aby 'spark' był dostępny w notebooku, w którym tworzysz obiekt Config
        self.spark_session.conf.set("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{self.spark_session.conf.get('spark.databricks.clusterUsageTags.azureSubscriptionId')}/oauth2/token")