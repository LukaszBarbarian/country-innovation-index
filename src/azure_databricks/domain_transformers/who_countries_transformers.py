# azure_databricks/transformers/who/who_countries_raw_to_bronze_transformer.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Any, Dict, Optional

from src.azure_databricks.common.transformers.base_data_transformer import BaseDataTransformer
from src.common.enums.file_format import FileFormat
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.domain_source import DomainSource # <-- Dodany import
from src.azure_databricks.common.factories.transformer_registry import TransformerRegistry

# --- ZAKTUALIZOWANY DEKORATOR ---
@TransformerRegistry.register_transformer_decorator(
    source_id="who_countries",
    target_layer=ETLLayer.BRONZE,
    domain_source=DomainSource.WHO # <-- DODANO domain_source
)
class WHO_Countries_RawToBronzeTransformer(BaseDataTransformer):
    """
    Transformator odpowiedzialny za przetwarzanie danych o krajach WHO z warstwy Raw do Bronze.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.read_options = self.specific_config.get("read_options", {"header": "true", "inferSchema": "true", "delimiter": ","})
        self.partition_cols = self.specific_config.get("partition_cols", ["load_date"])

        print(f"WHO_Countries_RawToBronzeTransformer zainicjowany dla źródła '{self.source_name}', domeny '{self.domain_source.value}'. Opcje odczytu: {self.read_options}")

    def transform(self) -> DataFrame:
        print(f"Rozpoczynam transformację danych WHO Countries z RAW do BRONZE dla '{self.source_name}' w domenie '{self.domain_source.value}'...")

        raw_data_location = f"{self.config.RAW_CONTAINER}{self.source_name}/countries.csv"

        print(f"Odczytuję surowe dane z: {raw_data_location} w formacie {FileFormat.CSV.value} z opcjami: {self.read_options}")

        df_raw = self._read_data(
            path=raw_data_location,
            file_format=FileFormat.CSV,
            options=self.read_options
        )

        if df_raw.isEmpty():
            print(f"Ostrzeżenie: Brak danych do przetworzenia dla '{self.source_name}' w lokalizacji '{raw_data_location}'. Zwracam pusty DataFrame.")
            return self.spark.createDataFrame([], df_raw.schema)

        transformed_df = df_raw.select(
            col("Country").alias("country_name"),
            col("ISO_code").alias("iso_code"),
            col("Population").cast("long").alias("population"),
            lit(self.source_name).alias("source_system"),
            current_timestamp().alias("load_timestamp"),
            current_timestamp().cast("date").alias("load_date")
        )

        print(f"Transformacja danych WHO Countries zakończona. Liczba przetworzonych rekordów: {transformed_df.count()}")
        return transformed_df