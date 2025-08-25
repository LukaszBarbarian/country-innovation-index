# src/silver/factories/model_builder_factory.py
from src.common.registers.model_builder_registry import ModelBuilderRegistry
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.model_type import ModelType
from src.common.builders.base_model_builder import BaseModelBuilder
from src.common.factories.base_factory import BaseFactoryFromRegistry


class ModelBuilderFactory(BaseFactoryFromRegistry[ModelType, BaseModelBuilder]):
    """
    Fabryka odpowiedzialna za tworzenie instancji BaseModelBuilderów.
    Wykorzystuje ModelBuilderRegistry do pobierania odpowiednich klas.
    """
    @classmethod
    def get_registry(cls) -> BaseRegistry[ModelType, BaseModelBuilder]:
        """
        Zwraca instancję (lub klasę) ModelBuilderRegistry,
        która przechowuje zarejestrowane klasy ModelBuilderów.
        """
        return ModelBuilderRegistry()