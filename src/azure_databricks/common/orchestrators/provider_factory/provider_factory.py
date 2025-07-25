from typing import Any
from pyspark.sql import SparkSession

# Importy klas dla obu środowisk
from src.common.enums.env import Env
from src.azure_databricks.common.configuration.config import ProjectConfig
from src.azure_databricks.common.persister.persister import Persister
from src.azure_databricks.common.reader.reader import DataReader
from src.azure_databricks.common.structures.structure_builder import StructureBuilder
from src.azure_databricks.utils.local.local_config import LocalPersister
from src.azure_databricks.utils.local.local_structure_builder import LocalStrucrtureBuilder

class ProviderFactory:
    """
    Fabryka, która tworzy obiekty specyficzne dla danego środowiska.
    """
    def __init__(self, spark: SparkSession, dbutils_obj: Any, config: ProjectConfig, env: Env):
        self.spark = spark
        self.dbutils = dbutils_obj
        self.config = config
        self.env = env
        print(f"ProviderFactory zainicjowany dla środowiska '{self.env}'.")

    def get_persister(self):
        if self.env == Env.LOCAL:
            return LocalPersister(self.spark, self.config, self.get_structure_builder())
        return Persister(self.spark, self.config, self.get_structure_builder())

    def get_structure_builder(self):
        if self.env == Env.LOCAL:
            return LocalStrucrtureBuilder(self.spark, self.dbutils, self.config)
        return StructureBuilder(self.spark, self.dbutils, self.config)

    def get_data_reader(self):
        return DataReader(self.spark, self.config)