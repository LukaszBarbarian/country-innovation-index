from enum import Enum


class WriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"