# src/silver/model_builders/country_model_builder.py

from typing import cast
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.enums.model_type import ModelType
from src.common.enums.domain_source import DomainSource
from src.silver.context.silver_context import SilverLayerContext
from src.silver.models.country_model import CountryModel
from injector import inject
from src.common.registers.model_builder_registry import ModelBuilderRegistry


@ModelBuilderRegistry.register(ModelType.COUNTRY)
class CountryModelBuilder(BaseModelBuilder):

    async def build(self):
        """
        Główna logika budowania modelu Country.
        1. Pobiera zarejestrowane readery dla ModelType.COUNTRIES.
        2. Czyta dane z każdego źródła (Bronze/Bronze Plus).
        3. Standaryzuje kolumny dla każdego źródła.
        4. Scala wszystkie standaryzowane dane.
        5. Wykonuje unifikację i deduplikację, tworząc finalny model.
        """

        context = cast(SilverLayerContext, self._context)
        
        nobelprize_reader = self.get_reader(DomainSource.NOBELPRIZE)
        result = nobelprize_reader.load_data()
                    
        return None
    
    async def normalize(self, model):
        return model
    
    async def enrich(self, model):
        return model
    
    async def transform(self, model):
        return model