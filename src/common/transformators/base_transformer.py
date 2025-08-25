from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseTransformer(ABC):
    """Abstrakcyjna klasa dla transformatorów danych w ramach DomainSource."""

    @abstractmethod
    async def transform(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """Etap transformacji (selekcja, kolumn, wstępna obróbka)."""
        raise NotImplementedError

    @abstractmethod
    async def normalize(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """Etap normalizacji (ujednolicanie formatu, tworzenie kluczy)."""
        raise NotImplementedError

    @abstractmethod
    async def enrich(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """Etap wzbogacania danych (opcjonalny)."""
        raise NotImplementedError