from enum import Enum

class ETLLayer(Enum):
    UNKNOWN = "unknown"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
