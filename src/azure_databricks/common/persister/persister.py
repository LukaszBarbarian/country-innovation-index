from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional
from delta.tables import DeltaTable 
from enum import Enum 

from azure_databricks.common.structures.structure_builder import StructureBuilder
from azure_databricks.common.configuration.config import ProjectConfig
from azure_databricks.common.enums.etl_layers import ETLLayer
from azure_databricks.common.enums.write_mode import WriteMode


class Persister:
    """
    Klasa odpowiedzialna za fizyczne zapisywanie danych (DataFrame) do tabel Delta Lake,
    z pełną obsługą Unity Catalog i tworzenia zewnętrznych tabel.
    Wykorzystuje StructureBuilder do zapewnienia istnienia schematów w Unity Catalog.
    """
    def __init__(self, spark: SparkSession, config: ProjectConfig, structure_builder: StructureBuilder):
        self.spark = spark
        self.config = config
        self.structure_builder = structure_builder
        
        # Pobieramy nazwę katalogu Unity Catalog z konfiguracji projektu
        self.catalog_name = self.config.get_unity_catalog_name() 
        print(f"Inicjowanie Persistera z Unity Catalog: '{self.catalog_name}'.")

    def persist_df(
        self,
        df: DataFrame,
        base_table_name: str, # Np. "who_countries" - nazwa tabeli bez katalogu i schematu
        layer: ETLLayer,      # Np. ETLLayer.BRONZE - warstwa ETL jako Enum
        mode: WriteMode = WriteMode.APPEND, # Tryb zapisu jako Enum
        partition_cols: Optional[List[str]] = None, # Kolumny do partycjonowania
        merge_keys: Optional[List[str]] = None # Klucze dla operacji MERGE
    ):
        """
        Zapisuje Spark DataFrame do określonej tabeli zewnętrznej Delta Lake w Unity Catalog.
        Najpierw zapewnia istnienie schematu w Unity Catalog za pomocą StructureBuilder.
        
        Args:
            df (DataFrame): Spark DataFrame do zapisania.
            base_table_name (str): Bazowa nazwa tabeli (np. "who_countries").
            layer (ETLLayer): Warstwa ETL (Bronze, Silver, Gold).
            mode (WriteMode): Tryb zapisu ("append", "overwrite", "merge").
            partition_cols (Optional[List[str]]): Lista kolumn do partycjonowania tabeli.
            merge_keys (Optional[List[str]]): Lista kluczy głównych dla operacji MERGE.
                                               Wymagane, gdy mode == WriteMode.MERGE.
        """
        if df.isEmpty():
            print(f"DataFrame dla tabeli '{base_table_name}' w warstwie '{layer.value}' jest puste. Pomijam zapis.")
            return

        # Upewniamy się, że partition_cols jest zawsze listą
        if partition_cols is None:
            partition_cols = []

        # Nazwa schematu w Unity Catalog jest równoznaczna z warstwą (np. "bronze", "silver")
        schema_name = layer.value 
        # Pełna, trójpoziomowa nazwa tabeli w Unity Catalog (np. "main.bronze.who_countries")
        full_table_name_uc = f"{self.catalog_name}.{schema_name}.{base_table_name}"

        # 1. Zapewniamy, że schemat (baza danych) istnieje w Unity Catalog
        # StructureBuilder jest odpowiedzialny za `CREATE SCHEMA IF NOT EXISTS`.
        self.structure_builder.ensure_schema_exists(self.catalog_name, schema_name)
        
        # 2. Określamy pełną ścieżkę do przechowywania plików danych dla tabeli zewnętrznej
        # To jest ścieżka w Twoim Data Lake (ADLS Gen2)
        full_table_location = ""
        if layer == ETLLayer.BRONZE:
            full_table_location = f"{self.config.get_bronze_location()}{base_table_name}/"
        elif layer == ETLLayer.SILVER:
            full_table_location = f"{self.config.get_silver_location()}{base_table_name}/"
        elif layer == ETLLayer.GOLD:
            full_table_location = f"{self.config.get_gold_location()}{base_table_name}/"
        else:
            # Ta walidacja jest w zasadzie nadmiarowa dzięki typowaniu Enum, ale dobra dla bezpieczeństwa
            raise ValueError(f"Nieobsługiwana warstwa: {layer}. Musi być ETLLayer.BRONZE, ETLLayer.SILVER lub ETLLayer.GOLD.")

        print(f"Rozpoczynam zapis do tabeli UC: '{full_table_name_uc}' (ścieżka: '{full_table_location}') w trybie '{mode.value}'.")
        
        # 3. Wykonujemy faktyczny zapis danych w zależności od trybu
        if mode == WriteMode.MERGE:
            if not merge_keys:
                raise ValueError("Dla trybu 'merge' musisz podać 'merge_keys'.")
            
            # Sprawdzamy, czy tabela (meta i dane) już istnieje w Unity Catalog.
            # Używamy _jsparkSession.catalog().tableExists() do sprawdzania rejestracji w Metastore.
            table_exists_in_uc = self.spark._jsparkSession.catalog().tableExists(full_table_name_uc)
            
            if not table_exists_in_uc:
                print(f"Tabela '{full_table_name_uc}' nie istnieje w Unity Catalog. Tworzę nową zewnętrzną tabelę Delta.")
                # Pierwsze wstawienie: tworzymy zewnętrzną tabelę Delta, rejestrując ją w UC.
                # Używamy "append" aby po prostu wstawić dane, tworząc jednocześnie schemat tabeli.
                (df.write.format("delta")
                    .mode("append") 
                    .option("path", full_table_location) # WAŻNE: podaj ścieżkę dla tabeli zewnętrznej
                    .partitionBy(*partition_cols)
                    .saveAsTable(full_table_name_uc)) # Rejestrujemy pod pełną nazwą UC
                print(f"Utworzono zewnętrzną tabelę '{full_table_name_uc}' i zapisano początkowe dane.")
            else:
                print(f"Tabela '{full_table_name_uc}' istnieje. Wykonuję operację MERGE.")
                # Tabela już istnieje, możemy bezpiecznie wykonać MERGE.
                # DeltaTable.forPath wymaga ścieżki, nie pełnej nazwy UC.
                delta_table = DeltaTable.forPath(self.spark, full_table_location)
                
                # Budowanie warunku JOIN dla MERGE
                join_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in merge_keys])
                
                (delta_table.alias("target")
                    .merge(
                        source=df.alias("source"),
                        condition=join_condition
                    )
                    .whenMatchedUpdateAll() # Aktualizuj wszystkie pasujące kolumny
                    .whenNotMatchedInsertAll() # Wstaw nowe wiersze
                    .execute())
                print(f"Dane scalone do '{full_table_name_uc}'.")
        else: # Tryby APPEND i OVERWRITE
            writer = df.write.format("delta").mode(mode.value) # Używamy .value z Enuma WriteMode
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Zapisz dane do wskazanej ścieżki i zarejestruj tabelę zewnętrzną w Unity Catalog
            writer.option("path", full_table_location).saveAsTable(full_table_name_uc)
            print(f"Dane zapisane w trybie '{mode.value}' do '{full_table_name_uc}'.")

        print(f"Zapis do tabeli '{full_table_name_uc}' w warstwie '{layer.value}' zakończony pomyślnie.")
