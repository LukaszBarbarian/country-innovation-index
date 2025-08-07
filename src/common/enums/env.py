from enum import Enum


class Env(Enum):
    UNKNOWN = "unknown"
    LOCAL = "local"
    DEV = "dev"
    PROD = "prod"