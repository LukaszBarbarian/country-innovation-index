from enum import Enum

class FileFormat(Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    DELTA = "delta"