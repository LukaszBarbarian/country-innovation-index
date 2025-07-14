# azure_databricks/orchestrators/base_domain_orchestrator.py

from pyspark.sql import SparkSession
from typing import Any, Dict, Optional
from abc import ABC, abstractmethod

from azure_databricks.common.configuration.config import ProjectConfig
from azure_databricks.common.persister.persister import Persister
from azure_databricks.common.reader.reader import DataReader
from azure_databricks.common.structures.structure_builder import StructureBuilder
from azure_databricks.common.factories.transformer_factory import TransformerFactory 
from azure_databricks.common.enums.etl_layers import ETLLayer 
from azure_databricks.common.enums.env import Env

class BaseDomainOrchestrator(ABC):
    def __init__(self, 
                 spark: SparkSession, 
                 dbutils_obj: Any, 
                 config: ProjectConfig,
                 persister: Persister,
                 data_reader: DataReader,
                 structure_builder: StructureBuilder,
                 env: Env = Env.DEV):
        self.spark = spark
        self.dbutils = dbutils_obj
        self.config = config
        self.persister = persister
        self.data_reader = data_reader
        self.structure_builder = structure_builder
        self.env = env
        
        self.transformer_factory = TransformerFactory(
            spark=self.spark,
            persister=self.persister,
            data_reader=self.data_reader,
            config=self.config
        )
        print(f"BaseDomainOrchestrator zainicjowany dla środowiska '{self.env}'.")

    @abstractmethod
    async def execute(self, target_etl_layer: ETLLayer):
        """
        Abstrakcyjna metoda, która musi zostać zaimplementowana przez podklasy.
        Zawiera logikę uruchamiania wszystkich transformacji dla danej domeny dla określonej warstwy ETL.
        """
        pass