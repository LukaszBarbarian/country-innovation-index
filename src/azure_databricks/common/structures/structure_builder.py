from pyspark.sql import SparkSession
from typing import Optional, List, Any

from src.azure_databricks.common.configuration.config import ProjectConfig
from src.common.enums.etl_layers import ETLLayer

class StructureBuilder:
    def __init__(self, spark: SparkSession, dbutils_obj: Any, config: ProjectConfig):
        self.spark = spark
        self.dbutils = dbutils_obj 
        self.config = config 
        
        # Pobieramy nazwy i ID z ProjectConfig
        self.catalog_name = self.config.get_unity_catalog_name() 
        self.access_connector_id = self.config.databricks_access_connector_id # ID Managed Identity/Service Principal
        self.storage_account_name = self.config.datalake_storage_account_name # Nazwa konta storage
        
        # Definiujemy nazwy dla Storage Credentials i External Locations
        # Upewnij się, że nazwy są unikalne w Unity Catalog
        self.storage_credential_name = f"sc_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        
        self.external_location_raw = f"el_raw_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        self.external_location_bronze = f"el_bronze_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        self.external_location_silver = f"el_silver_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        self.external_location_gold = f"el_gold_{self.config.env}_{self.storage_account_name.replace('.', '_').replace('-', '_')}"
        
        print("Inicjowanie StructureBuilder.")

    def _execute_sql(self, sql_command: str, success_message: str, error_message: str, check_exists_command: Optional[str] = None):
        """
        Pomocnicza metoda do wykonywania komend SQL i obsługi błędów,
        z opcjonalnym sprawdzeniem istnienia przed wykonaniem CREATE.
        """
        if check_exists_command:
            try:
                self.spark.sql(check_exists_command)
                print(f"Obiekt już istnieje: {check_exists_command.split(' ')[2]}. Pomijam tworzenie.")
                return True # Obiekt już istnieje, więc sukces
            except Exception:
                # Obiekt nie istnieje, przechodzimy do tworzenia
                print(f"Obiekt nie istnieje, tworzę: {check_exists_command.split(' ')[2]}.")
        
        try:
            print(f"Wykonuję: {sql_command}")
            self.spark.sql(sql_command)
            print(success_message)
            return True
        except Exception as e:
            # W Unity Catalog, błędy "ALREADY_EXISTS" często oznaczają sukces
            # Możesz to obsłużyć bardziej elegancko, sprawdzając treść błędu
            # lub po prostu ignorując ten konkretny typ błędu, jeśli używasz IF NOT EXISTS
            if "ALREADY_EXISTS" in str(e).upper():
                print(f"Ostrzeżenie: Obiekt już istnieje, mimo próby utworzenia. Ignoruję błąd: {e}")
                return True
            print(f"BŁĄD: {error_message}. Szczegóły: {e}")
            raise # Rzuć wyjątek ponownie, jeśli błąd jest krytyczny

    def initialize_databricks_environment(self):
        """
        Główna metoda do kompleksowej inicjalizacji środowiska Databricks i Unity Catalog.
        """
        print("\n--- Rozpoczynam inicjalizację środowiska Databricks i Unity Catalog ---")
        
        # 1. Sprawdź i utwórz katalog Unity Catalog
        self.ensure_catalog_exists(self.catalog_name)

        # 2. Utwórz / użyj Storage Credential
        # WAŻNE: ensure_storage_credential_exists używa MANAGED IDENTITY
        # Upewnij się, że 'self.access_connector_id' to poprawne ID Twojego Managed Identity/Service Principal
        self.ensure_storage_credential_exists(
            name=self.storage_credential_name,
            azure_application_id=self.access_connector_id
        )

        # 3. Utwórz / użyj External Locations dla każdej warstwy
        self.ensure_external_location_exists(
            name=self.external_location_raw,
            path=self.config.get_raw_location(), # Używamy getterów z configa
            credential_name=self.storage_credential_name
        )
        self.ensure_external_location_exists(
            name=self.external_location_bronze,
            path=self.config.get_bronze_location(),
            credential_name=self.storage_credential_name
        )
        self.ensure_external_location_exists(
            name=self.external_location_silver,
            path=self.config.get_silver_location(),
            credential_name=self.storage_credential_name
        )
        self.ensure_external_location_exists(
            name=self.external_location_gold,
            path=self.config.get_gold_location(),
            credential_name=self.storage_credential_name
        )

        # 4. Utwórz schematy (bazy danych) w Unity Catalog
        self.ensure_schema_exists(self.catalog_name, ETLLayer.BRONZE.value)
        self.ensure_schema_exists(self.catalog_name, ETLLayer.SILVER.value)
        self.ensure_schema_exists(self.catalog_name, ETLLayer.GOLD.value)

        print("\n--- Inicjalizacja środowiska Databricks i Unity Catalog zakończona ---")

    def ensure_catalog_exists(self, catalog_name: str):
        """
        Zapewnia, że katalog Unity Catalog istnieje.
        """
        self._execute_sql(
            sql_command=f"CREATE CATALOG IF NOT EXISTS {catalog_name}",
            success_message=f"Katalog '{catalog_name}' istnieje lub został utworzony.",
            error_message=f"Nie udało się utworzyć katalogu '{catalog_name}'. Sprawdź uprawnienia (Account Admin lub CREATE CATALOG)."
        )

    def ensure_schema_exists(self, catalog_name: str, schema_name: str):
        """
        Zapewnia, że schemat (baza danych) istnieje w podanym katalogu Unity Catalog.
        """
        full_schema_name = f"{catalog_name}.{schema_name}"
        self._execute_sql(
            sql_command=f"CREATE SCHEMA IF NOT EXISTS {full_schema_name}",
            success_message=f"Schemat '{full_schema_name}' istnieje lub został utworzony.",
            error_message=f"Nie udało się utworzyć schematu '{full_schema_name}'. Sprawdź uprawnienia CREATE SCHEMA."
        )

    def ensure_storage_credential_exists(self, name: str, azure_application_id: str):
        """
        Zapewnia, że Storage Credential istnieje w Unity Catalog, używając Managed Identity.
        """
        self._execute_sql(
            sql_command=f"""
                CREATE STORAGE CREDENTIAL {name}
                MANAGED IDENTITY '{azure_application_id}'
            """,
            success_message=f"Storage Credential '{name}' utworzono pomyślnie.",
            error_message=f"Nie udało się utworzyć Storage Credential '{name}'. Sprawdź uprawnienia CREATE STORAGE CREDENTIAL i poprawność ID.",
            check_exists_command=f"DESCRIBE STORAGE CREDENTIAL {name}" # Sprawdzamy istnienie przed próbą CREATE
        )
            
    def ensure_external_location_exists(self, name: str, path: str, credential_name: str):
        """
        Zapewnia, że External Location istnieje w Unity Catalog.
        """
        self._execute_sql(
            sql_command=f"""
                CREATE EXTERNAL LOCATION {name}
                URL '{path}'
                WITH CREDENTIAL {credential_name}
            """,
            success_message=f"External Location '{name}' utworzono pomyślnie dla ścieżki '{path}'.",
            error_message=f"Nie udało się utworzyć External Location '{name}' dla ścieżki '{path}'. Sprawdź uprawnienia CREATE EXTERNAL LOCATION i poprawność ścieżki/credentiala.",
            check_exists_command=f"DESCRIBE EXTERNAL LOCATION {name}" # Sprawdzamy istnienie przed próbą CREATE
        )

    def get_full_table_location(self, layer: ETLLayer, base_table_name: str) -> str:
        """
        Zwraca pełną ścieżkę ADLS Gen2 dla danej warstwy i bazowej nazwy tabeli.
        Ta metoda jest używana przez Persistera do opcji 'path' w saveAsTable,
        która nadal wymaga pełnej ścieżki abfss://.
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
        Ta nazwa jest używana, gdy tworzysz tabele zewnętrzne w Unity Catalog
        (np. CREATE TABLE ... LOCATION 'adl://...').
        """
        if layer == ETLLayer.BRONZE:
            return self.external_location_bronze
        elif layer == ETLLayer.SILVER:
            return self.external_location_silver
        elif layer == ETLLayer.GOLD:
            return self.external_location_gold
        else:
            raise ValueError(f"Nieznana warstwa ETL: {layer}")