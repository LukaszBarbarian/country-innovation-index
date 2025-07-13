import asyncio
from typing import List, Any, Dict, Tuple, Optional

from pyspark.sql import SparkSession

from azure_databricks.config.project_config import ProjectConfig
from azure_databricks.persisters.persister import Persister
# Usuwamy import TransformerFactory i ETLOrchestrator, bo WHO_ETLOrchestrator będzie używał ich wewnętrznie
from azure_databricks.common.etl_layers import ETLLayer

# Importujemy nową klasę bazową
from azure_databricks.common.base_etl_orchestrator import BaseETLOrchestrator 

# Importujemy SOURCE_NAME'y i klasy transformatorów
# WAŻNE: Musisz zaimportować konkretne klasy transformatorów, aby ich dekoratory @TransformerRegistry.register zadziałały!
# Pamiętaj, że do listy WHO_SOURCES dodaliśmy specific_config. Dostosuj transformatory, aby je wykorzystywały.
from azure_databricks.transformers.who.who_countries_raw_to_bronze_transformer import WHO_Countries_RawToBronzeTransformer, SOURCE_NAME as WHO_COUNTRIES_SOURCE_NAME_RB
from azure_databricks.transformers.who.who_vaccinations_raw_to_bronze_transformer import WHO_Vaccinations_RawToBronzeTransformer, SOURCE_NAME as WHO_VACCINATIONS_SOURCE_NAME_RB
from azure_databricks.transformers.who.who_deaths_raw_to_bronze_transformer import WHO_Deaths_RawToBronzeTransformer, SOURCE_NAME as WHO_DEATHS_SOURCE_NAME_RB

from azure_databricks.transformers.who.who_countries_bronze_to_silver_transformer import WHO_Countries_BronzeToSilverTransformer, SOURCE_NAME as WHO_COUNTRIES_SOURCE_NAME_BS
from azure_databricks.transformers.who.who_vaccinations_bronze_to_silver_transformer import WHO_Vaccinations_BronzeToSilverTransformer, SOURCE_NAME as WHO_VACCINATIONS_SOURCE_NAME_BS
from azure_databricks.transformers.who.who_deaths_bronze_to_silver_transformer import WHO_Deaths_BronzeToSilverTransformer, SOURCE_NAME as WHO_DEATHS_SOURCE_NAME_BS

# Ponownie zaimportuj TransformerFactory i ETLOrchestrator, ale TYLKO w tym pliku.
# WHO_ETLOrchestrator nadal ich używa wewnętrznie.
from azure_databricks.factories.transformer_factory import TransformerFactory
from azure_databricks.orchestrators.etl_orchestrator import ETLOrchestrator


class WHO_ETLOrchestrator(BaseETLOrchestrator): # DZIEDZICZY Z NOWEJ KLASY BAZOWEJ
    # Definiujemy wszystkie SOURCE_NAME'y, które należą do rodziny WHO
    WHO_SOURCES: List[Tuple[str, Dict[str, Any]]] = [
        # Zaktualizowane specific_config dla poszczególnych źródeł, jeśli mają kolumnę wartości
        (WHO_COUNTRIES_SOURCE_NAME_RB, {}), # Kraje mogą nie mieć "value_column"
        (WHO_VACCINATIONS_SOURCE_NAME_RB, {"value_column": "total_vaccinations"}),
        (WHO_DEATHS_SOURCE_NAME_RB, {"value_column": "deaths_count"}),
    ]

    def __init__(self, spark: SparkSession, dbutils_obj: Any, etl_layer: ETLLayer, env: str = "dev"):
        super().__init__(spark, dbutils_obj, env) # Wywołujemy konstruktor klasy bazowej
        
        self.etl_layer = etl_layer
        self.config = ProjectConfig(dbutils_obj=self.dbutils_obj, spark_session=self.spark, env=self.env)
        
        print(f"Inicjowanie WHO_ETLOrchestrator dla warstwy: {self.etl_layer.value}")

    async def run(self):
        """
        Implementacja metody run() z BaseETLOrchestrator.
        Zarządza uruchomieniem odpowiednich transformatorów WHO dla danej warstwy.
        """
        print(f"\n--- Rozpoczynam orkiestrację ETL dla danych WHO w warstwie {self.etl_layer.value} ---")

        transformer_specs_for_who: List[Tuple[str, ETLLayer, Optional[Dict[str, Any]]]] = []
        for source_name, default_specific_config in self.WHO_SOURCES:
            # Tworzymy pełne specyfikacje dla GŁÓWNEGO ETLOrchestratora
            transformer_specs_for_who.append((source_name, self.etl_layer, default_specific_config))

        if not transformer_specs_for_who:
            print(f"Brak zdefiniowanych transformatorów WHO dla warstwy {self.etl_layer.value}. Zakończono.")
            return

        print(f"Przekazuję następujące specyfikacje do głównego ETLOrchestratora dla warstwy {self.etl_layer.value}:")
        for spec in transformer_specs_for_who:
            print(f" - {spec[0]} -> {spec[1].value} (Config: {spec[2]})")

        # Tworzymy i uruchamiamy główny ETLOrchestrator, który przetworzy listę transformatorów
        main_orchestrator = ETLOrchestrator(
            spark=self.spark,
            dbutils_obj=self.dbutils_obj,
            transformer_specs=transformer_specs_for_who, # WAŻNA ZMIANA: To jest nadal lista transformatorów
            env=self.env
        )
        await main_orchestrator.run()

        print(f"--- Orkiestracja ETL dla danych WHO w warstwie {self.etl_layer.value} zakończona ---")