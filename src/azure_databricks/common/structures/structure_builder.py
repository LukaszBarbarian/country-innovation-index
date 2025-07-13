from pyspark.sql import SparkSession
from typing import Optional, List, Any

from azure_databricks.common.configuration.config import ProjectConfig
from azure_databricks.common.enums.etl_layers import ETLLayer

class StructureBuilder:
    def __init__(self, spark: SparkSession, dbutils_obj: Any, config: ProjectConfig):
        self.spark = spark
        self.dbutils = dbutils_obj # Przekazujemy dbutils_obj do StructureBuilder
        self.config = config     # Przekazujemy ProjectConfig
        
        # Pobieramy nazwę katalogu UC z configa
        self.catalog_name = self.config.get_unity_catalog_name() 

        # Pobieramy ID konektora dostępowego i nazwę konta storage z configa (poprzez BaseCloudConfig)
        self.access_connector_id = self.config.databricks_access_connector_id
        self.storage_account_name = self.config.datalake_storage_account_name
        
        # Definiujemy nazwy dla Storage Credentials i External Locations
        # Możesz dostosować te nazwy, np. dodając 'env'
        self.storage_credential_name = f"sc_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        
        # Nazwy external locations mogą być per-warstwowe lub ogólne
        self.external_location_raw = f"el_raw_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        self.external_location_bronze = f"el_bronze_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        self.external_location_silver = f"el_silver_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        self.external_location_gold = f"el_gold_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        
        print("Inicjowanie StructureBuilder.")

    def _execute_sql(self, sql_command: str, success_message: str, error_message: str):
        """Pomocnicza metoda do wykonywania komend SQL i obsługi błędów."""
        try:
            print(f"Wykonuję: {sql_command}")
            self.spark.sql(sql_command)
            print(success_message)
            return True
        except Exception as e:
            print(f"BŁĄD: {error_message}. Szczegóły: {e}")
            # Rzuć wyjątek ponownie, jeśli błąd jest krytyczny
            raise

    def initialize_databricks_environment(self):
        """
        Główna metoda do kompleksowej inicjalizacji środowiska Databricks i Unity Catalog.
        """
        print("\n--- Rozpoczynam inicjalizację środowiska Databricks i Unity Catalog ---")
        
        # 1. Sprawdź i utwórz katalog Unity Catalog
        self.ensure_catalog_exists(self.catalog_name)

        # 2. Utwórz / użyj Storage Credential
        # To wymaga uprawnień do tworzenia Storage Credential w Unity Catalog.
        # Wartość `self.config.databricks_access_connector_id` to Application ID dla Service Principal
        # lub Managed Identity, który ma dostęp do ADLS Gen2.
        self.ensure_storage_credential_exists(
            name=self.storage_credential_name,
            azure_application_id=self.access_connector_id # ID konektora dostępowego
        )

        # 3. Utwórz / użyj External Locations dla każdej warstwy
        # Pamiętaj, że pełne ścieżki do kontenerów są w ProjectConfig
        self.ensure_external_location_exists(
            name=self.external_location_raw,
            path=self.config.RAW_CONTAINER,
            credential_name=self.storage_credential_name
        )
        self.ensure_external_location_exists(
            name=self.external_location_bronze,
            path=self.config.BRONZE_CONTAINER,
            credential_name=self.storage_credential_name
        )
        self.ensure_external_location_exists(
            name=self.external_location_silver,
            path=self.config.SILVER_CONTAINER,
            credential_name=self.storage_credential_name
        )
        self.ensure_external_location_exists(
            name=self.external_location_gold,
            path=self.config.GOLD_CONTAINER,
            credential_name=self.storage_credential_name
        )

        # 4. Utwórz schematy (bazy danych) w Unity Catalog
        # Ważne: do tworzenia schematów potrzebujesz uprawnień CREATE SCHEMA w danym katalogu.
        self.ensure_schema_exists(self.catalog_name, ETLLayer.BRONZE.value)
        self.ensure_schema_exists(self.catalog_name, ETLLayer.SILVER.value)
        self.ensure_schema_exists(self.catalog_name, ETLLayer.GOLD.value)

        print("\n--- Inicjalizacja środowiska Databricks i Unity Catalog zakończona ---")

    def ensure_catalog_exists(self, catalog_name: str):
        """
        Zapewnia, że katalog Unity Catalog istnieje.
        Wymaga uprawnień do tworzenia katalogów (Account Admin lub przypisana rola).
        """
        self._execute_sql(
            sql_command=f"CREATE CATALOG IF NOT EXISTS {catalog_name}",
            success_message=f"Katalog '{catalog_name}' istnieje lub został utworzony.",
            error_message=f"Nie udało się utworzyć katalogu '{catalog_name}'. Sprawdź uprawnienia (Account Admin lub CREATE CATALOG)."
        )

    def ensure_schema_exists(self, catalog_name: str, schema_name: str):
        """
        Zapewnia, że schemat (baza danych) istnieje w podanym katalogu Unity Catalog.
        Wymaga uprawnień CREATE SCHEMA w katalogu.
        """
        full_schema_name = f"{catalog_name}.{schema_name}"
        self._execute_sql(
            sql_command=f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}",
            success_message=f"Schemat '{full_schema_name}' istnieje lub został utworzony.",
            error_message=f"Nie udało się utworzyć schematu '{full_schema_name}'. Sprawdź uprawnienia CREATE SCHEMA."
        )

    def ensure_storage_credential_exists(self, name: str, azure_application_id: str):
        """
        Zapewnia, że Storage Credential istnieje w Unity Catalog.
        Używa Service Principal (Azure Application ID) lub Managed Identity.
        Wymaga uprawnień CREATE STORAGE CREDENTIAL.
        """
        # Możesz dodać logikę do sprawdzania, czy credential istnieje,
        # ale CREATE STORAGE CREDENTIAL IF NOT EXISTS nie jest obsługiwane.
        # Dlatego bezpieczniej jest próbować go utworzyć i obsłużyć błąd,
        # jeśli już istnieje.
        sql_command = f"""
            CREATE STORAGE CREDENTIAL IF NOT EXISTS {name}
            MANAGED IDENTITY '{azure_application_id}';
            -- Alternatywnie dla Service Principal:
            -- SERVICE PRINCIPAL '{azure_application_id}'
            -- CLIENT SECRET '{Secrets.get_secret(self.dbutils, self.config.DATABRICKS_SECRET_SCOPE, "service-principal-client-secret-key")}';
        """
        # Sprawdź istnienie ręcznie, bo "IF NOT EXISTS" nie działa dla STORAGE CREDENTIAL
        try:
            self.spark.sql(f"DESCRIBE STORAGE CREDENTIAL {name}")
            print(f"Storage Credential '{name}' już istnieje.")
        except Exception:
            print(f"Tworzę Storage Credential '{name}'.")
            self._execute_sql(
                sql_command=f"""
                    CREATE STORAGE CREDENTIAL {name}
                    MANAGED IDENTITY '{azure_application_id}'
                """,
                success_message=f"Storage Credential '{name}' utworzono pomyślnie.",
                error_message=f"Nie udało się utworzyć Storage Credential '{name}'. Sprawdź uprawnienia CREATE STORAGE CREDENTIAL i poprawność ID."
            )
            
    def ensure_external_location_exists(self, name: str, path: str, credential_name: str):
        """
        Zapewnia, że External Location istnieje w Unity Catalog.
        Mapuje ścieżkę ADLS Gen2 na nazwę w UC, używając Storage Credential.
        Wymaga uprawnień CREATE EXTERNAL LOCATION.
        """
        # Podobnie jak przy Storage Credential, CREATE EXTERNAL LOCATION IF NOT EXISTS nie działa.
        try:
            self.spark.sql(f"DESCRIBE EXTERNAL LOCATION {name}")
            print(f"External Location '{name}' ({path}) już istnieje.")
        except Exception:
            print(f"Tworzę External Location '{name}' dla ścieżki '{path}'.")
            self._execute_sql(
                sql_command=f"""
                    CREATE EXTERNAL LOCATION {name}
                    URL '{path}'
                    WITH CREDENTIAL {credential_name}
                """,
                success_message=f"External Location '{name}' utworzono pomyślnie dla ścieżki '{path}'.",
                error_message=f"Nie udało się utworzyć External Location '{name}' dla ścieżki '{path}'. Sprawdź uprawnienia CREATE EXTERNAL LOCATION i poprawność ścieżki/credentiala."
            )

    # Te metody są teraz mniej istotne, ponieważ StructureBuilder zajmuje się Unity Catalog.
    # Tabele zewnętrzne w Unity Catalog odwołują się do External Locations,
    # a nie bezpośrednio do pełnych ścieżek abfss://...
    # Metody Persistera będą nadal używać pełnych ścieżek abfss://... dla opcji 'path',
    # ale będą one podlegać pod External Locations.
    def get_full_table_location(self, layer: ETLLayer, base_table_name: str) -> str:
        """
        Zwraca pełną ścieżkę ADLS Gen2 dla danej warstwy i bazowej nazwy tabeli.
        Używane przez Persistera do opcji 'path' w saveAsTable.
        """
        if layer == ETLLayer.BRONZE:
            return f"{self.config.get_bronze_location()}{base_table_name}/"
        elif layer == ETLLayer.SILVER:
            return f"{self.config.get_silver_location()}{base_table_name}/"
        elif layer == ETLLayer.GOLD:
            return f"{self.config.get_gold_location()}{base_table_name}/"
        else:
            raise ValueError(f"Nieznana warstwa ETL: {layer}")

    def get_external_location_name(self, layer: ETLLayer) -> str:
        """
        Zwraca nazwę External Location w Unity Catalog dla danej warstwy.
        Przydatne, jeśli kiedykolwiek będziesz potrzebować odwoływać się do EL z kodu.
        """
        if layer == ETLLayer.RAW:
            return self.external_location_raw
        elif layer == ETLLayer.BRONZE:
            return self.external_location_bronze
        elif layer == ETLLayer.SILVER:
            return self.external_location_silver
        elif layer == ETLLayer.GOLD:
            return self.external_location_gold
        else:
            raise ValueError(f"Nieznana warstwa ETL: {layer}")