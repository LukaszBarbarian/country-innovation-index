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
    A data reader for patents, which loads a CSV file from the location
    specified in the context.
    
    It inherits from `BaseDataReader` to provide a standardized way of
    reading data from the Bronze layer for the Silver layer processing.
    """

    def _load_from_source(self, all_readers: Optional[List[SummaryResultBase]]) -> Dict[str, DataFrame]:
        """
        Loads data from the source paths provided in the summary results.

        This method reads the 'patents' CSV file, ensuring that the loaded data
        is a valid DataFrame. It uses Spark's `read_csv_https` to handle the
        file path and automatically infers the schema and uses the header.

        Args:
            all_readers (Optional[List[SummaryResultBase]]): A list of summary
                                                            results from the Bronze layer run.

        Returns:
            Dict[str, DataFrame]: A dictionary of loaded DataFrames, keyed by
                                  the dataset name. Returns an empty dictionary if
                                  no data is found or an error occurs.
        """
        if not all_readers:
            print("No results for DomainSource.PATENTS in the context.")
            return {}

        dataframes_dict: Dict[str, DataFrame] = {}
        for result in all_readers:
            if result.is_valid and result.dataset_name == "patents":
                for path in result.output_paths:
                    try:
                        df = self._spark.read_csv_https(path, header=True, inferSchema=True)
                        dataframes_dict[result.dataset_name] = df
                        print(f"Loaded data for dataset '{result.dataset_name}' from file: {path}")
                    except Exception as e:
                        print(f"Error loading file {path}: {e}")
        return dataframes_dict