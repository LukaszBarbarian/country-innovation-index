from enum import Enum
from typing import Literal


class ApiType(Enum):
    WHO = "WHO"


WhoApiQueryType = Literal["indicator_data", "list_indicators", "list_dimensions", "list_dimension_values"]
