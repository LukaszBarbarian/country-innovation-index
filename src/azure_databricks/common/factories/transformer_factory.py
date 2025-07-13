from pyspark.sql import SparkSession
from typing import Type, Optional, Dict, Any

from azure_databricks.common.persister.persister import Persister
from azure_databricks.common.configuration.config import ProjectConfig
from azure_databricks.common.transformers.base_data_transformer import BaseDataTransformer
from azure_databricks.common.factories.transformer_registry import TransformerRegistry
from azure_databricks.who.transformers.who_countries_transformator import WHO_Countries_RawToBronzeTransformer

class TransformerFactory:
    def __init__(self, spark: SparkSession, persister: Persister, config: ProjectConfig):
        self.spark = spark
        self.persister = persister
        self.config = config
        print("Inicjowanie TransformerFactory.")

    # Metoda przyjmuje teraz tylko dataset_name
    def create_transformer(self, dataset_name: str, specific_config: Optional[Dict[str, Any]] = None) -> BaseDataTransformer:
        """
        Tworzy instancję transformatora dla danego zestawu danych.
        """
        transformer_class = TransformerRegistry.get_dataset_transformer_class(dataset_name)
        
        print(f"Tworzę instancję transformatora dla zestawu danych '{dataset_name}' ({transformer_class.__name__})")

        instance = transformer_class(
            spark=self.spark,
            persister=self.persister,
            config=self.config,
            specific_config=specific_config # Nadal przekazujemy specific_config
        )
        return instance