from enum import Enum


class DomainSourceType(Enum):
    UNKNOWN = "unknown"
    API = "api"
    STATIC_FILE = "static_file"