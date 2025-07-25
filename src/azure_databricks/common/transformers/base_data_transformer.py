# azure_databricks/common/base_data_transformer.py

from pyspark.sql import SparkSession, DataFrame
from typing import Any, Dict, Optional, List
from abc import ABC, abstractmethod

from src.azure_databricks.common.persister.persister import Persister
from src.azure_databricks.common.configuration.config import ProjectConfig
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.file_format import FileFormat
from src.common.enums.write_mode import WriteMode
from src.azure_databricks.common.reader.reader import DataReader
from src.common.enums.domain_source import DomainSource # <-- Dodany import

class BaseDataTransformer(ABC):
    def __init__(self,
                 spark: SparkSession,
                 persister: Persister,
                 data_reader: DataReader,
                 config: ProjectConfig,
                 source_name: str,
                 target_layer: ETLLayer,
                 domain_source: DomainSource, # <-- DODANY PARAMETR
                 specific_config: Optional[Dict[str, Any]] = None):
        self.spark = spark
        self.persister = persister
        self.data_reader = data_reader
        self.config = config
        self.source_name = source_name
        self.target_layer = target_layer
        self.domain_source = domain_source # <-- PRZYPISANIE
        self.specific_config = specific_config if specific_config is not None else {}
        print(f"BaseDataTransformer zainicjowany dla źródła: '{self.source_name}', warstwy docelowej: '{self.target_layer.value}', domeny: '{self.domain_source.value}' z konfiguracją: {self.specific_config}")

    async def process(self):
        """
        Główna metoda orkiestrująca proces ETL (Extract, Transform, Load).
        Ta metoda powinna być wywoływana przez orkiestratora ETL.
        """
        print(f"Rozpoczynam przetwarzanie danych dla '{self.source_name}' w domenie '{self.domain_source.value}' do warstwy '{self.target_layer.value}'...")

        # 1. Transformacja danych
        transformed_df = self.transform()

        # 2. Zapis danych do odpowiedniej warstwy (delegowanie do Persistera)
        if self.target_layer == ETLLayer.BRONZE:
            self.persister.persist_to_bronze(transformed_df, self.source_name, WriteMode.OVERWRITE_PARTITIONS)
        elif self.target_layer == ETLLayer.SILVER:
            # Pamiętaj, że dla MERGE musisz przekazać merge_keys
            self.persister.persist_to_silver(transformed_df, self.source_name, WriteMode.MERGE, merge_keys=["id"])
        elif self.target_layer == ETLLayer.GOLD:
            self.persister.persist_to_gold(transformed_df, self.source_name, WriteMode.OVERWRITE_PARTITIONS)
        else:
            raise ValueError(f"Nieznana lub nieobsługiwana warstwa docelowa: {self.target_layer.value}")

        print(f"Przetwarzanie danych dla '{self.source_name}' w domenie '{self.domain_source.value}' do warstwy '{self.target_layer.value}' zakończone.")

    @abstractmethod
    def transform(self) -> DataFrame:
        """
        Abstrakcyjna metoda, która musi zostać zaimplementowana przez klasy dziedziczące.
        Zawiera logikę transformacji danych.
        """
        pass

    def _read_data(self, path: str, file_format: FileFormat, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        """Odczytuje dane z podanej ścieżki i formatu."""
        return self.data_reader.read_data(path, file_format, options)