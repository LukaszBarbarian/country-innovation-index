# azure_databricks/common/persister/persister.py

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from typing import Any, Dict, Optional, List

from azure_databricks.common.configuration.config import ProjectConfig
from azure_databricks.common.enums.etl_layers import ETLLayer
from azure_databricks.common.enums.write_mode import WriteMode
from azure_databricks.common.structures.structure_builder import StructureBuilder

class Persister:
    def __init__(self, spark: SparkSession, config: ProjectConfig, structure_builder: StructureBuilder):
        self.spark = spark
        self.config = config
        self.structure_builder = structure_builder
        print("Persister zainicjowany.")

    def _get_full_table_details(self, etl_layer: ETLLayer, base_table_name: str) -> tuple[str, str]:
        """Helper to get full table name and path."""
        full_table_name = f"{self.config.get_unity_catalog_name()}.{etl_layer.value}.{base_table_name}"
        full_path = self.structure_builder.get_full_table_location(etl_layer, base_table_name)
        return full_table_name, full_path

    def persist_to_bronze(self, df: DataFrame, base_table_name: str, mode: WriteMode, partition_cols: Optional[List[str]] = None):
        """
        Zapisuje DataFrame do warstwy Bronze jako tabelę Delta Lake.
        Obsługiwane tryby: APPEND, OVERWRITE, OVERWRITE_PARTITIONS.
        """
        full_table_name, full_path = self._get_full_table_details(ETLLayer.BRONZE, base_table_name)

        print(f"Zapisuję do Bronze: '{full_table_name}' w lokalizacji '{full_path}' w trybie '{mode.value}'.")
        
        # Tworzymy obiekt DataFrameWriter
        writer = df.write.format("delta")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        if mode == WriteMode.APPEND:
            writer.mode("append").option("path", full_path).saveAsTable(full_table_name)
        elif mode == WriteMode.OVERWRITE:
            writer.mode("overwrite").option("path", full_path).saveAsTable(full_table_name)
        elif mode == WriteMode.OVERWRITE_PARTITIONS:
            if not partition_cols:
                raise ValueError("Dla trybu OVERWRITE_PARTITIONS w Bronze wymagane są 'partition_cols'.")
            writer.mode("overwrite").option("replaceWhere", "true").option("path", full_path).saveAsTable(full_table_name)
            # Uwaga: option("replaceWhere", "true") działa na partition columns.
            # Alternatywnie: writer.mode("overwrite").option("path", full_path).saveAsTable(full_table_name)
            # i wtedy data_frame powinien zawierać tylko dane dla partycji, które chcemy nadpisać.
        else:
            raise ValueError(f"Nieobsługiwany tryb zapisu '{mode.value}' dla warstwy Bronze.")
            
        print(f"Zapis do Bronze '{full_table_name}' zakończony.")

    def persist_to_silver(self, df: DataFrame, base_table_name: str, mode: WriteMode, 
                          partition_cols: Optional[List[str]] = None, merge_keys: Optional[List[str]] = None):
        """
        Zapisuje DataFrame do warstwy Silver jako tabelę Delta Lake, z obsługą MERGE.
        Obsługiwane tryby: MERGE, OVERWRITE, OVERWRITE_PARTITIONS.
        """
        full_table_name, full_path = self._get_full_table_details(ETLLayer.SILVER, base_table_name)

        print(f"Zapisuję do Silver: '{full_table_name}' w lokalizacji '{full_path}' w trybie '{mode.value}'.")

        if mode == WriteMode.MERGE:
            if not merge_keys:
                raise ValueError("Dla trybu MERGE w Silver wymagane są 'merge_keys'.")

            # Sprawdzamy, czy tabela istnieje, aby obsłużyć pierwsze uruchomienie
            try:
                # Próbujemy odwołać się do tabeli Delta
                delta_table = DeltaTable.forPath(self.spark, full_path)
                print(f"Tabela Silver '{full_table_name}' istnieje. Wykonuję MERGE.")
            except Exception:
                # Jeśli tabela nie istnieje, tworzymy ją z pełnego DataFrame'u (początkowy load)
                print(f"Tabela Silver '{full_table_name}' nie istnieje, tworzę nową.")
                writer = df.write.format("delta").mode(WriteMode.OVERWRITE.value)
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                writer.option("path", full_path).saveAsTable(full_table_name)
                # Ponowne załadowanie tabeli Delta po jej utworzeniu
                delta_table = DeltaTable.forPath(self.spark, full_path)

            merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
            print(f"Wykonuję operację MERGE dla '{full_table_name}' z warunkiem: '{merge_condition}'.")
            
            delta_table.alias("target").merge(
                source=df.alias("source"),
                condition=merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            print(f"Operacja MERGE dla '{full_table_name}' zakończona.")
            
        elif mode == WriteMode.OVERWRITE_PARTITIONS:
            if not partition_cols:
                raise ValueError("Dla trybu OVERWRITE_PARTITIONS w Silver wymagane są 'partition_cols'.")
            
            writer = df.write.format("delta").mode(WriteMode.OVERWRITE.value)
            writer = writer.option("overwriteSchema", "true") # Używaj z ostrożnością!
            writer = writer.option("replaceWhere", "true") # Nadpisuje tylko te partycje, które są w DF
            writer = writer.partitionBy(*partition_cols)
            writer.option("path", full_path).saveAsTable(full_table_name)
            print(f"Zapis do Silver '{full_table_name}' w trybie OVERWRITE_PARTITIONS zakończony.")
            
        elif mode == WriteMode.OVERWRITE or mode == WriteMode.APPEND:
            writer = df.write.format("delta").mode(mode.value)
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.option("path", full_path).saveAsTable(full_table_name)
            print(f"Zapis do Silver '{full_table_name}' w trybie '{mode.value}' zakończony.")
        else:
            raise ValueError(f"Nieobsługiwany tryb zapisu '{mode.value}' dla warstwy Silver.")

    def persist_to_gold(self, df: DataFrame, base_table_name: str, mode: WriteMode, 
                        partition_cols: Optional[List[str]] = None, merge_keys: Optional[List[str]] = None): # <-- DODANO merge_keys
        """
        Zapisuje DataFrame do warstwy Gold jako tabelę Delta Lake.
        Obsługiwane tryby: MERGE, OVERWRITE, OVERWRITE_PARTITIONS.
        """
        full_table_name, full_path = self._get_full_table_details(ETLLayer.GOLD, base_table_name)

        print(f"Zapisuję do Gold: '{full_table_name}' w lokalizacji '{full_path}' w trybie '{mode.value}'.")
        
        if mode == WriteMode.MERGE:
            if not merge_keys:
                raise ValueError("Dla trybu MERGE w Gold wymagane są 'merge_keys'.")
            
            # Sprawdzamy, czy tabela istnieje, aby obsłużyć pierwsze uruchomienie
            try:
                delta_table = DeltaTable.forPath(self.spark, full_path)
                print(f"Tabela Gold '{full_table_name}' istnieje. Wykonuję MERGE.")
            except Exception:
                print(f"Tabela Gold '{full_table_name}' nie istnieje, tworzę nową.")
                writer = df.write.format("delta").mode(WriteMode.OVERWRITE.value)
                if partition_cols: # Partycjonowanie podczas pierwszego zapisu
                    writer = writer.partitionBy(*partition_cols)
                writer.option("path", full_path).saveAsTable(full_table_name)
                delta_table = DeltaTable.forPath(self.spark, full_path) # Ponowne załadowanie

            merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
            print(f"Wykonuję operację MERGE dla '{full_table_name}' z warunkiem: '{merge_condition}'.")
            
            delta_table.alias("target").merge(
                source=df.alias("source"),
                condition=merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            print(f"Operacja MERGE dla '{full_table_name}' zakończona.")

        elif mode == WriteMode.OVERWRITE_PARTITIONS:
            if not partition_cols:
                raise ValueError("Dla trybu OVERWRITE_PARTITIONS w Gold wymagane są 'partition_cols'.")
            
            writer = df.write.format("delta").mode(WriteMode.OVERWRITE.value)
            writer = writer.option("overwriteSchema", "true") # Używaj z ostrożnością!
            writer = writer.option("replaceWhere", "true") # Nadpisuje tylko te partycje, które są w DF
            writer = writer.partitionBy(*partition_cols)
            writer.option("path", full_path).saveAsTable(full_table_name)
            print(f"Zapis do Gold '{full_table_name}' w trybie OVERWRITE_PARTITIONS zakończony.")

        elif mode == WriteMode.OVERWRITE or mode == WriteMode.APPEND:
            writer = df.write.format("delta").mode(mode.value)
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.option("path", full_path).saveAsTable(full_table_name)
            print(f"Zapis do Gold '{full_table_name}' w trybie '{mode.value}' zakończony.")
        else:
            raise ValueError(f"Nieobsługiwany tryb zapisu '{mode.value}' dla warstwy Gold.")
            
        print(f"Zapis do Gold '{full_table_name}' zakończony.")