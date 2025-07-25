# azure_databricks/orchestrators/main_etl_orchestrator.py

import asyncio
from typing import List, Any, Optional, Dict, Type
from pyspark.sql import SparkSession

from src.azure_databricks.common.configuration.config import ProjectConfig
from src.azure_databricks.common.persister.persister import Persister
from src.azure_databricks.common.reader.reader import DataReader
from src.azure_databricks.common.structures.structure_builder import StructureBuilder
from src.common.enums.etl_layers import ETLLayer 
from src.common.enums.domain_source import DomainSource 
from src.azure_databricks.common.orchestrators.domain_orchestrator_registry import DomainOrchestratorRegistry 
from src.azure_databricks.common.orchestrators.base_domain_orchestrator import BaseDomainOrchestrator
from src.common.enums.env import Env
from src.azure_databricks.utils.local.local_config import LocalPersister
from src.azure_databricks.utils.local.local_structure_builder import LocalStrucrtureBuilder
from src.azure_databricks.common.orchestrators.provider_factory.provider_factory import ProviderFactory

import src.azure_databricks.common.orchestrators.register_all_domain_orchestrators 

class MainETLOrchestrator:
    def __init__(self, spark: SparkSession, dbutils_obj: Any, config: ProjectConfig, env: Env = Env.DEV):
        self.spark = spark
        self.dbutils = dbutils_obj
        self.env = env
        self.config = config
        
        provider_factory = ProviderFactory(self.spark, self.dbutils, self.config, self.env)

        self.structure_builder = provider_factory.get_structure_builder()
        self.persister = provider_factory.get_persister()
        self.data_reader = provider_factory.get_data_reader()
        
        print(f"MainETLOrchestrator zainicjowany dla środowiska '{self.env}'.")

    async def execute_etl_layer_pipeline(self, target_etl_layer: ETLLayer, specific_domains: Optional[List[DomainSource]] = None):
        """
        Uruchamia pipeline ETL dla określonej warstwy ETL, iterując przez odpowiednie domenowe orkiestatory.
        Args:
            target_etl_layer (ETLLayer): Docelowa warstwa ETL do przetworzenia (np. BRONZE, SILVER, GOLD).
            specific_domains (Optional[List[DomainSource]]): Opcjonalna lista konkretnych domen do uruchomienia
                                                            dla tej warstwy. Jeśli None, uruchamia wszystkie zarejestrowane
                                                            dla danej warstwy.
        """
        print(f"\n--- Rozpoczynam główny pipeline ETL dla warstwy '{target_etl_layer.value}' w środowisku '{self.env}' ---")
        
        self.structure_builder.initialize_databricks_environment() 

        all_registered_for_layer = DomainOrchestratorRegistry.get_all_registered_orchestrators_for_layer(target_etl_layer)
        
        domain_orchestrators_to_run: Dict[DomainSource, Type[BaseDomainOrchestrator]] = {}

        if specific_domains:
            for domain in specific_domains:
                if domain in all_registered_for_layer:
                    domain_orchestrators_to_run[domain] = all_registered_for_layer[domain]
                else:
                    print(f"Ostrzeżenie: Domenowy orkiestrator '{domain.value}' nie jest zarejestrowany dla warstwy '{target_etl_layer.value}'. Zostanie pominięty.")
        else:
            domain_orchestrators_to_run = all_registered_for_layer

        if not domain_orchestrators_to_run:
            print(f"Brak domenowych orkiestratorów do uruchomienia dla warstwy '{target_etl_layer.value}'.")
            return

        for domain_source, orch_class in domain_orchestrators_to_run.items():
            print(f"\nŁaduję i uruchamiam orkiestrator dla domeny: {domain_source.value} do warstwy '{target_etl_layer.value}'...")
            
            domain_orchestrator = orch_class(
                spark=self.spark,
                dbutils_obj=self.dbutils,
                config=self.config,
                persister=self.persister,
                data_reader=self.data_reader,
                structure_builder=self.structure_builder,
                env=self.env
            )

            await domain_orchestrator.execute(target_etl_layer)
            print(f"Orkiestrator dla domeny {domain_source.value} do warstwy '{target_etl_layer.value}' zakończony.")

        print(f"\n--- Główny pipeline ETL dla warstwy '{target_etl_layer.value}' zakończony w środowisku '{self.env}' ---")