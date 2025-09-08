# src/silver/di_modules.py
from injector import Module, singleton, Binder, provider
from src.common.enums.model_type import ModelType
from src.common.factories.model_builder_factory import ModelBuilderFactory
from src.common.di.di_module import DIModule


class BuildersModule(DIModule):
    def configure(self, binder: Binder) -> None:
        for model_type in ModelType:
            try:
                builder_class = ModelBuilderFactory.get_class(model_type)

                binder.bind(builder_class, to=builder_class, scope=singleton)
            
            except KeyError:
                print(f"Ostrzeżenie: Brak buildera dla typu modelu '{model_type.name}'.")
                continue

