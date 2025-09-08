# src/silver/loaders/manual_data_loader.py
from typing import Optional, Dict
from injector import inject
from pyspark.sql import DataFrame
import os

from src.common.enums.domain_source import DomainSource
from src.common.spark.spark_service import SparkService
from src.silver.context.silver_context import SilverContext


class ManualDataReader:
    @inject
    def __init__(self, spark: SparkService, context: SilverContext):
        self._context = context
        self._spark = spark

    def load_for_dataset(self, domain_source: DomainSource, dataset_name: str) -> Optional[DataFrame]:
        """
        Ładuje konkretny dataset manualny dla danej domeny i zwraca DataFrame.
        Obsługuje pliki JSON i CSV na podstawie rozszerzenia.
        """
        file_path = None
        # Wyszukujemy ścieżkę do pliku na podstawie domeny i nazwy datasetu
        for path in self._context.manifest.manual_data_paths:
            if path.domain_source == domain_source and path.dataset_name == dataset_name:
                file_path = path.file_path
                break

        if not file_path:
            print(f"Błąd: Nie znaleziono ścieżki dla datasetu manualnego: '{domain_source.name}:{dataset_name}'.")
            return None
        
        try:
            # Określamy rozszerzenie pliku, aby wybrać odpowiednią metodę odczytu
            _, file_extension = os.path.splitext(file_path)
            
            if file_extension.lower() == '.json':
                df = self._spark.read_json_https(file_path)
            elif file_extension.lower() == '.csv':
                # Dodajemy opcję inferSchema, aby Spark automatycznie wykrył typy danych
                # oraz header=True, zakładając, że pierwszy wiersz to nagłówki kolumn
                df = self._spark.read_csv_https(file_path)
            else:
                print(f"Błąd: Nieobsługiwane rozszerzenie pliku '{file_extension}' dla pliku '{file_path}'.")
                return None
            
            print(f"Załadowano manualny dataset '{dataset_name}' z pliku: {file_path}")
            return df

        except Exception as e:
            print(f"Błąd podczas ładowania pliku manualnego '{file_path}': {e}")
            return None