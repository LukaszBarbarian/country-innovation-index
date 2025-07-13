from dataclasses import dataclass
import dbutils

class Secrets:
    @classmethod
    def get_secret(cls, secret_scope: str, secret_key: str):
        """Pobiera sekret z podanego zakresu i klucza."""
        try:
            return dbutils.secrets.get(scope=secret_scope, key=secret_key)
        except Exception as e:
            raise ValueError(f"Nie udało się pobrać sekretu '{secret_key}' z zakresu '{secret_scope}'. Sprawdź uprawnienia i istnienie sekretu. Błąd: {e}")


@dataclass
class Config:
    DATABRICKS_SECRET_SCOPE: str = "demosur_dev_secret_scope"
    DATABRICKS_ACCESS_CONNECTOR_ID_KEY: str = "databricks-access-connector-id"
    DATALAKE_STORAGE_ACCOUNT_NAME_KEY: str = "datalake-storage-account-name"

    _BRONZE_CONTAINER_PREFIX: str = "bronze"
    _SILVER_CONTAINER_PREFIX: str = "silver"
    _GOLD_CONTAINER_PREFIX: str = "gold"


    @property
    def datalake_storage_account_name(self) -> str:
        """Pobiera dynamiczną nazwę konta storage z sekretów."""
        return Secrets.get_secret(self.DATABRICKS_SECRET_SCOPE, self.DATALAKE_STORAGE_ACCOUNT_NAME_KEY)

    @property
    def databricks_access_connector_id(self) -> str:
        """Pobiera ID konektora dostępowego z sekretów."""
        return Secrets.get_secret(self.DATABRICKS_SECRET_SCOPE, self.DATABRICKS_ACCESS_CONNECTOR_ID_KEY)

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

    def configure_spark_for_adls_gen2(self, spark):
        """
        Konfiguruje Sparka do dostępu do ADLS Gen2 za pomocą Service Principal/Access Connector.
        """
        spark.conf.set("fs.azure.account.auth.type", "OAuth")
        spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set("fs.azure.account.oauth2.client.id", self.databricks_access_connector_id)
        spark.conf.set("fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{spark.conf.get('spark.databricks.clusterUsageTags.azureSubscriptionId')}/oauth2/token")