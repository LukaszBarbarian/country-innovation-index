from abc import ABC, abstractmethod
from typing import List, Any
from src.common.models.raw_data import RawData

class ApiLoader(ABC):
    @abstractmethod
    async def load(self) -> List[RawData]:
        pass