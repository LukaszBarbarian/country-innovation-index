import asyncio
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List

from pyspark.sql import SparkSession, DataFrame
from azure_databricks.common.enums.write_mode import WriteMode
from azure_databricks.common.configuration.config import ProjectConfig
from azure_databricks.common.persister.persister import Persister
from azure_databricks.common.enums.file_format import FileFormat
from azure_databricks.common.enums.etl_layers import ETLLayer

class BaseDataTransformer(ABC):
    def __init__(self, spark: SparkSession, persister: Persister, config: ProjectConfig,
                 source_id: str, 
                 specific_config: Optional[Dict[str, Any]] = None):
        
        self.spark = spark
        self.persister = persister # Persister jest wstrzykiwany i przechowywany
        self.config = config
        self.source_id = source_id 
        self.specific_config = specific_config if specific_config is not None else {}

        print(f"Inicjowanie BaseDataTransformer dla źródła: {self.source_id}")

    @abstractmethod
    async def process(self):
        pass

    def _read_data(self, path: str, file_format: FileFormat, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        """Deleguje odczyt."""
        return self.spark.read.format(file_format.value).options(**(options if options else {})).load(path)

    # METODY ZAPISU DELEGUJĄCE DO PERSISTERA
    def _persist_to_bronze(self, df: DataFrame, base_table_name: str, mode: WriteMode, partition_cols: Optional[List[str]] = None):
        """Persists DataFrame to Bronze layer by delegating to the Persister."""
        if partition_cols is None:
            partition_cols = []
            
        print(f"Deleguję zapis {df.count()} rekordów do Persistera (warstwa Bronze, tabela: {base_table_name}, tryb: {mode.value}).")
        self.persister.persist_df(
            df=df, 
            base_table_name=base_table_name, 
            layer=ETLLayer.BRONZE, 
            mode=mode, 
            partition_cols=partition_cols
        )
        print(f"Zapis do Bronze (tabela: {base_table_name}) zakończony (delegowano do Persistera).")

    def _persist_to_silver(self, df: DataFrame, base_table_name: str, mode: WriteMode, 
                                     partition_cols: Optional[List[str]] = None, primary_keys: Optional[List[str]] = None):
        """Persists DataFrame to Silver layer by delegating to the Persister."""
        if partition_cols is None:
            partition_cols = []
            
        print(f"Deleguję zapis {df.count()} rekordów do Persistera (warstwa Silver, tabela: {base_table_name}, tryb: {mode.value}).")
        if mode == WriteMode.MERGE and not primary_keys:
            raise ValueError("For MERGE mode in Silver, 'primary_keys' must be provided.")

        self.persister.persist_df(
            df=df, 
            base_table_name=base_table_name, 
            layer=ETLLayer.SILVER, 
            mode=mode, 
            partition_cols=partition_cols,
            merge_keys=primary_keys 
        )
        print(f"Zapis do Silver (tabela: {base_table_name}) zakończony (delegowano do Persistera).")