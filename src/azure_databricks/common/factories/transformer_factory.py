# azure_databricks/factories/transformer_factory.py

from typing import Dict, Type, Any, Optional
from pyspark.sql import SparkSession
from src.azure_databricks.common.persister.persister import Persister
from src.azure_databricks.common.reader.reader import DataReader
from src.azure_databricks.common.configuration.config import ProjectConfig
from src.common.enums.etl_layers import ETLLayer
from src.azure_databricks.common.transformers.base_data_transformer import BaseDataTransformer
from src.azure_databricks.common.factories.transformer_registry import TransformerRegistry

class TransformerFactory:
    def __init__(self, spark: SparkSession, persister: Persister, data_reader: DataReader, config: ProjectConfig):
        self.spark = spark
        self.persister = persister
        self.data_reader = data_reader
        self.config = config

    def create_transformer(self, source_id: str, etl_layer: ETLLayer, specific_config: Optional[Dict[str, Any]] = None) -> BaseDataTransformer:
        # Zmieniamy kolejność wywołań, aby najpierw pobrać wszystkie niezbędne informacje
        registered_transformer_class = TransformerRegistry.get_transformer_class(source_id)
        registered_target_layer = TransformerRegistry.get_target_layer(source_id)
        registered_domain_source = TransformerRegistry.get_domain_source(source_id) # <-- POBIERAMY DOMAIN_SOURCE

        if registered_target_layer != etl_layer:
            raise ValueError(
                f"Niezgodność warstwy dla transformatora '{source_id}'. "
                f"Oczekiwano '{etl_layer.value}', zarejestrowano '{registered_target_layer.value}'."
            )
        
        # Sprawdzenie spójności, jeśli to konieczne (np. czy target_layer zgadza się z ETL_Layer z parametru)
        # ... (możesz dodać tu bardziej rygorystyczne sprawdzenia) ...

        return registered_transformer_class( # Używamy pobranej klasy
            spark=self.spark,
            persister=self.persister,
            data_reader=self.data_reader,
            config=self.config,
            source_name=source_id,
            target_layer=etl_layer, # Nadal przekazujemy z parametru, lub możesz użyć registered_target_layer
            domain_source=registered_domain_source, # <-- PRZEKAZUJEMY DO KONSTRUKTORA
            specific_config=specific_config
        )