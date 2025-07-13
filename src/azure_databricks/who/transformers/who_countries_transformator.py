from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, to_date, input_file_name
import asyncio
from typing import Optional, Dict, Any

from azure_databricks.common.transformers.base_data_transformer import BaseDataTransformer
from azure_databricks.common.enums.file_format import FileFormat
from azure_databricks.common.persister.persister import Persister
from azure_databricks.common.configuration.config import ProjectConfig
from azure_databricks.common.factories.transformer_registry import TransformerRegistry
from azure_databricks.common.enums.etl_layers import ETLLayer

SOURCE_NAME = "who_countries" 
TARGET_LAYER = ETLLayer.BRONZE

@TransformerRegistry.register_transformer(SOURCE_NAME, TARGET_LAYER)
class WHO_Countries_RawToBronzeTransformer(BaseDataTransformer):
    def __init__(self, spark: SparkSession, persister: Persister, config: ProjectConfig,
                 specific_config: Optional[Dict[str, Any]] = None):
        
        super().__init__(spark, persister, config, SOURCE_NAME, specific_config) 

        self.raw_data_input_path = f"{self.config.get_raw_location()}{self.source_id}/*/*.json" 
        
        self.file_format = FileFormat.JSON
        self.read_options = {"multiline": "true", "inferSchema": "true"}
        self.bronze_table_name = f"bronze_{self.source_id}"
        self.bronze_partition_cols = ["load_date"] 

        print(f"Inicjowanie WHO_Countries_RawToBronzeTransformer (źródło Raw: {self.raw_data_input_path}, cel Bronze: {self.bronze_table_name})")

    async def process(self):
        # ... (reszta metody process() pozostaje bez zmian) ...
        print(f"\n--- Rozpoczynam proces WHO Raw -> Bronze dla danych o krajach ({self.source_id}) ---")
        
        raw_df = self._read_data(self.raw_data_input_path, self.file_format, self.read_options)
        
        if raw_df.isEmpty():
            print(f"Brak nowych danych do przetworzenia w Raw dla '{self.source_id}'. Zakończono.")
            return

        print(f"Odczytano {raw_df.count()} rekordów z Raw dla '{self.source_id}'. Wykonuję transformacje...")
        
        try:
            bronze_df = raw_df.select(
                col("id").cast("string").alias("country_id"), 
                col("name").cast("string").alias("country_name"),
                col("code").cast("string").alias("country_code"),
                col("continent").cast("string").alias("continent_name"),
            )
        except Exception as e:
            print(f"BŁĄD: Problem z wyborem kolumn z surowego schematu. Sprawdź schemat JSON. Błąd: {e}")
            raw_df.printSchema()
            raise

        bronze_df = bronze_df.withColumn("load_timestamp", current_timestamp()) \
                             .withColumn("load_date", to_date(current_timestamp())) \
                             .withColumn("source_system", lit(self.source_id)) \
                             .withColumn("file_path", input_file_name()) 

        initial_count = bronze_df.count()
        bronze_df = bronze_df.filter(col("country_id").isNotNull() & col("country_name").isNotNull())
        if bronze_df.count() < initial_count:
            print(f"Usunięto {initial_count - bronze_df.count()} wierszy z powodu brakujących 'country_id' lub 'country_name'.")
        
        print("\nSchema po transformacjach w Bronze (Countries):")
        bronze_df.printSchema()
        print("\nPrzykładowe 5 wierszy po transformacjach w Bronze (Countries):")
        bronze_df.show(5, truncate=False)

        await asyncio.sleep(0.5)

        self._persist_to_bronze(
            df=bronze_df,
            table_name=self.bronze_table_name,
            mode="append",
            partition_cols=self.bronze_partition_cols
        )
        
        print(f"--- Proces WHO Raw -> Bronze dla danych o krajach ({self.source_id}) zakończony ---")