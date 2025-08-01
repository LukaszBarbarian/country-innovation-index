from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from src.common.enums.domain_source import DomainSource
from src.common.enums.model_type import ModelType

class BaseDataReader(ABC):
    """
    Bazowa klasa abstrakcyjna dla wszystkich czytników danych (DataReaderów).
    Definiuje podstawowy kontrakt dla odczytu danych z różnych źródeł
    i przygotowania ich do dalszego przetwarzania.
    """

    def __init__(self, spark: SparkSession):
        """
        Inicjalizuje BaseDataReader.
        :param spark: Aktywna sesja Sparka używana do odczytu danych.
        """
        self.spark = spark

    @abstractmethod
    def get_source_domain(self) -> DomainSource:
        """
        Zwraca domenę źródłową (enum DomainSource), z której czytnik pobiera dane.
        """
        pass

    @abstractmethod
    def get_model_type_for_reader(self) -> ModelType:
        """
        Zwraca typ modelu (enum ModelType), dla którego ten czytnik dostarcza dane.
        Pomocne w automatycznym mapowaniu readerów do builderów.
        """
        pass

    @abstractmethod
    def read_data(self) -> DataFrame:
        """
        Abstrakcyjna metoda do odczytu danych.
        Implementacje tej metody powinny wczytać dane z konkretnego źródła
        i zwrócić je w postaci Spark DataFrame.
        """
        pass