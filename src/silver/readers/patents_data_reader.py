# src/silver/data_readers/patents_data_reader.py
from typing import Dict, Optional, List
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.common.enums.domain_source import DomainSource
from src.common.models.ingestion_result import IngestionResult
from src.common.models.models import SummaryResultBase
from src.common.readers.base_data_reader import BaseDataReader
from src.common.registers.data_reader_registry import DataReaderRegistry

@DataReaderRegistry.register(DomainSource.PATENTS)
class PatentsDataReader(BaseDataReader):
    """
    Czytnik danych dla patentów, wczytujący plik CSV z lokalizacji podanej w kontekście.
    """

    def _load_from_source(self, all_readers: Optional[List[SummaryResultBase]]) -> Dict[str, DataFrame]:
        if not all_readers:
            print("Brak wyników dla DomainSource.PATENTS w kontekście.")
            return {}

        dataframes_dict: Dict[str, DataFrame] = {}
        for result in all_readers:
            if result.is_valid and result.dataset_name == "patents":
                for path in result.output_paths:
                    try:
                        df = self._spark.read_csv_https(path, header=True, inferSchema=True)
                        dataframes_dict[result.dataset_name] = df
                        print(f"Załadowano dane dla datasetu '{result.dataset_name}' z pliku: {path}")
                    except Exception as e:
                        print(f"Błąd podczas ładowania pliku {path}: {e}")
        return dataframes_dict