from typing import Type, Dict, Callable
from azure_databricks.common.transformers.base_data_transformer import BaseDataTransformer

class TransformerRegistry:
    # Rejestr będzie mapował dataset_name (str) na klasę transformatora
    _registry: Dict[str, Type[BaseDataTransformer]] = {}

    @classmethod
    def register_dataset_transformer(cls, dataset_name: str) -> Callable[[Type[BaseDataTransformer]], Type[BaseDataTransformer]]:
        """
        Dekorator do rejestracji klas transformatorów danych (np. WHODatasetTransformer).
        Rejestruje transformator na podstawie unikalnej nazwy zestawu danych.
        """
        def decorator(transformer_class: Type[BaseDataTransformer]) -> Type[BaseDataTransformer]:
            if not issubclass(transformer_class, BaseDataTransformer):
                raise TypeError(f"Klasa {transformer_class.__name__} musi dziedziczyć po BaseDataTransformer.")
            
            if dataset_name in cls._registry:
                raise ValueError(f"Transformator dla zestawu danych '{dataset_name}' jest już zarejestrowany.")
            
            cls._registry[dataset_name] = transformer_class
            print(f"Zarejestrowano transformator dla zestawu danych: '{dataset_name}' -> {transformer_class.__name__}")
            return transformer_class
        return decorator

    @classmethod
    def get_dataset_transformer_class(cls, dataset_name: str) -> Type[BaseDataTransformer]:
        """
        Zwraca zarejestrowaną klasę transformatora dla danego zestawu danych.
        """
        transformer_class = cls._registry.get(dataset_name)
        if not transformer_class:
            raise ValueError(f"Brak zarejestrowanego transformatora dla zestawu danych: '{dataset_name}'")
        return transformer_class