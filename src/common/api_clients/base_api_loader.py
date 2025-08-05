from abc import ABC, abstractmethod
from typing import List, Any
from src.common.models.api_result import ApiResult

class ApiLoader(ABC):
    @abstractmethod
    async def load(self) -> ApiResult:
        pass