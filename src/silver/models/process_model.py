from dataclasses import dataclass
from typing import Optional

from src.common.enums.model_type import ModelType
from src.common.models.base_process import BaseProcessModel


@dataclass
class SilverProcessModel(BaseProcessModel):
    model_type: Optional[ModelType] = None


    def __post_init__(self):
        # Sprawdzamy, czy model_type ma wartość. Jeśli tak, to przypisujemy
        # ją do 'name' (pola z klasy bazowej).
        if self.model_type:
            self.name = self.model_type.value
        else:
            # Jeśli nie ma, możesz użyć wartości domyślnej
            self.name = "default_silver_name"