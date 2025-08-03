# src/silver/model_builders/country_model_builder.py

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource # Potrzebne do identyfikacji źródeł
from src.silver.models.city_model import CityModel
from typing import List, Type, Dict
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.country_model_builder import CountryModelBuilder


@ModelBuilderRegistry.register(ModelType.CITY)
class CityModelBuilder(BaseModelBuilder):
    def __init__(self, country_builder: CountryModelBuilder):
        self.country_builder = country_builder


    def _build(self, runtime_context):
        """
        Główna logika budowania modelu Country.
        1. Pobiera zarejestrowane readery dla ModelType.COUNTRIES.
        2. Czyta dane z każdego źródła (Bronze/Bronze Plus).
        3. Standaryzuje kolumny dla każdego źródła.
        4. Scala wszystkie standaryzowane dane.
        5. Wykonuje unifikację i deduplikację, tworząc finalny model.
        """

        
        return CityModel(None)