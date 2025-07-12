from pyspark.sql import DataFrame
from abc import ABC, abstractmethod
from typing import Optional, Dict


class DataPersister(ABC):
    def __init__(self, target_container_path: str):
         self.target_container_path = target_container_path


    def save_data_as_delta(self, df: DataFrame, table_name: str, mode: str = "overwrite", partition_by: Optional[list] = None):
        full_path = f"{self.target_container_path}{table_name.replace('.', '/')}/"

        writer = df.write.format("delta").mode(mode).option("path", full_path)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
            print(f"Partycjonowanie po kolumnach: {partition_by}")

        try:
            writer.saveAsTable(table_name)
        except Exception as e:
            raise # Przekaż błąd dalej