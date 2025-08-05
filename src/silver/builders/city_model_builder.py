# src/silver/model_builders/country_model_builder.py

from pyspark.sql import DataFrame, SparkSession
from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.enums.model_type import ModelType
from src.silver.models.city_model import CityModel
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.silver.builders.country_model_builder import CountryModelBuilder
from injector import inject, Injector
from src.silver.contexts.silver_context import SilverContext


@ModelBuilderRegistry.register(ModelType.CITY)
class CityModelBuilder(BaseModelBuilder):
    @inject
    def __init__(self,
                 spark: SparkSession,
                 injector: Injector,
                 context: SilverContext,
                 country_builder: CountryModelBuilder):
        
        super().__init__(context=context, injector=injector, spark=spark)
        self.country_builder = country_builder


    async def build(self):
        return CityModel(None)
    


    async def normalize(self, model):
        return await super().normalize(model)
    
    async def enrich(self, model):
        return await super().enrich(model)
    
    async def transform(self, model):
        return await super().transform(model)