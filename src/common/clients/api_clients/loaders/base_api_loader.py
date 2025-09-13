from abc import ABC, abstractmethod
from typing import List, Any
from src.common.models.raw_data import RawData

class ApiLoader(ABC):
    """
    An abstract base class for any class that loads data from an external API.
    It serves as a contract, ensuring all API loaders implement a consistent method for fetching data.
    """
    @abstractmethod
    async def load(self) -> List[RawData]:
        """
        Loads data from an API.

        This is an abstract asynchronous method that must be implemented by subclasses.
        It is designed for I/O-bound operations like network requests, allowing for
        non-blocking behavior.

        Returns:
            List[RawData]: A list of RawData objects containing the fetched information.
        """
        pass