from enum import Enum

class FileFormat(Enum):
    JSON = "json"
    CSV = "csv"
    BINARY = "binary"
    PARQUET = "parquet"
    DELTA = "delta"