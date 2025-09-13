from dataclasses import dataclass
from typing import Optional

from src.common.enums.model_type import ModelType
from src.common.models.base_process import BaseProcessModel


@dataclass
class SilverProcessModel(BaseProcessModel):
    """
    A dataclass representing a processed data model in the Silver ETL layer.

    This class extends `BaseProcessModel` and adds a specific `model_type` field.
    The `__post_init__` method automatically sets the `name` inherited from the
    base class to the value of the `model_type` enum, ensuring a consistent
    naming convention for Silver layer outputs.
    """
    model_type: Optional[ModelType] = None

    def __post_init__(self):
        """
        Post-initialization hook to set the `name` field based on `model_type`.
        """
        # We check if `model_type` has a value. If it does, we assign it
        # to 'name' (a field from the base class).
        if self.model_type:
            self.name = self.model_type.value
        else:
            # If not, you can use a default value.
            self.name = "default_silver_name"