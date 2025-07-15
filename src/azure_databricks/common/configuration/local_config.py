from typing import Any
from pyspark.sql import SparkSession
from src.azure_databricks.common.configuration.config import ProjectConfig
from src.azure_databricks.common.enums.env import Env

class LocalConfig(ProjectConfig):
    """
    Konfiguracja specyficzna dla lokalnego środowiska deweloperskiego.
    Dziedziczy po ProjectConfig, ale pomija kroki specyficzne dla Databricks.
    """

    def __init__(self, dbutils_obj: Any, spark_session: SparkSession):
        """
        Inicjalizuje konfigurację dla środowiska lokalnego.
        Parametr env jest tutaj na stałe ustawiony na Env.LOCAL.
        """
        # Wywołujemy konstruktor klasy bazowej ProjectConfig z Env.LOCAL
        super().__init__(
            dbutils_obj=dbutils_obj,
            spark_session=spark_session,
            env=Env.LOCAL
        )
        print("Lokalna konfiguracja została pomyślnie zainicjowana.")

    def _get_base_data_lake_path(self) -> str:
        """
        Zwraca lokalną ścieżkę do magazynu danych.
        W tym przypadku jest to katalog 'data' w głównym folderze projektu.
        """
        # Możesz dostosować tę ścieżkę do swoich potrzeb
        return "data/"

    def _get_unity_catalog_name(self) -> str:
        """
        Zwraca nazwę katalogu dla środowiska lokalnego.
        Ta nazwa jest używana do symulacji katalogu w lokalnym Sparku.
        """
        return "local_catalog"
    
    def _configure_spark_session(self):
        """
        Pomijamy konfigurację Spark Session, ponieważ w lokalnym
        środowisku nie używamy Unity Catalog.
        """
        print("Pominięto konfigurację katalogu Unity Catalog. To jest środowisko lokalne.")