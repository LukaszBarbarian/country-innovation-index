from dataclasses import dataclass
from typing import Optional

from src.common.enums.model_type_table import ModelTypeTable
from src.common.models.base_process import BaseProcessModel


@dataclass
class GoldProcessModel(BaseProcessModel):
    type: ModelTypeTable = None