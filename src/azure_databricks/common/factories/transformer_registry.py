# azure_databricks/factories/transformer_factory.py

from typing import Dict, Type, Any, Callable
from pyspark.sql import SparkSession
from src.azure_databricks.common.enums.etl_layers import ETLLayer
from src.azure_databricks.common.enums.domain_source import DomainSource
from src.azure_databricks.common.transformers.base_data_transformer import BaseDataTransformer

class TransformerRegistry:
    _registry: Dict[str, Type[BaseDataTransformer]] = {}
    _target_layer_map: Dict[str, ETLLayer] = {}
    _domain_source_map: Dict[str, DomainSource] = {}

    @classmethod
    def _register(cls, source_id: str, transformer_class: Type[BaseDataTransformer], target_layer: ETLLayer, domain_source: DomainSource):
        if not issubclass(transformer_class, BaseDataTransformer):
            raise TypeError(f"Klasa {transformer_class.__name__} musi dziedziczyć z BaseDataTransformer.")
        if source_id in cls._registry:
            print(f"Ostrzeżenie: Transformator dla '{source_id}' jest już zarejestrowany. Zostanie nadpisany.")
        cls._registry[source_id] = transformer_class
        cls._target_layer_map[source_id] = target_layer
        cls._domain_source_map[source_id] = domain_source
        print(f"Zarejestrowano transformator '{source_id}' (Domen: {domain_source.value}) do warstwy '{target_layer.value}'")

    @classmethod
    def register_transformer_decorator(cls, source_id: str, target_layer: ETLLayer, domain_source: DomainSource) -> Callable[[Type[BaseDataTransformer]], Type[BaseDataTransformer]]: # <-- DODANY PARAMETR
        """
        Dekorator do automatycznej rejestracji klas transformatorów.

        Args:
            source_id (str): Unikalny identyfikator źródła danych.
            target_layer (ETLLayer): Docelowa warstwa ETL, do której ten transformator zapisuje dane.
            domain_source (DomainSource): Domena, do której należy ten transformator.

        Returns:
            Callable: Dekorator, który przyjmuje klasę transformatora i ją rejestruje.
        """
        def decorator(transformer_class: Type[BaseDataTransformer]) -> Type[BaseDataTransformer]:
            cls._register(source_id, transformer_class, target_layer, domain_source) # <-- PRZEKAZUJEMY DO _register
            return transformer_class
        return decorator

    @classmethod
    def get_transformer_class(cls, source_id: str) -> Type[BaseDataTransformer]:
        transformer_class = cls._registry.get(source_id)
        if not transformer_class:
            raise ValueError(f"Transformator dla '{source_id}' nie jest zarejestrowany.")
        return transformer_class

    @classmethod
    def get_target_layer(cls, source_id: str) -> ETLLayer:
        target_layer = cls._target_layer_map.get(source_id)
        if not target_layer:
            raise ValueError(f"Warstwa docelowa dla transformatora '{source_id}' nie jest zarejestrowana.")
        return target_layer

    @classmethod
    def get_domain_source(cls, source_id: str) -> DomainSource: # <-- NOWA METODA DO POBIERANIA DOMAIN_SOURCE
        domain_source = cls._domain_source_map.get(source_id)
        if not domain_source:
            raise ValueError(f"Domena dla transformatora '{source_id}' nie jest zarejestrowana.")
        return domain_source

    @classmethod
    def get_all_registered_transformers(cls) -> Dict[str, Type[BaseDataTransformer]]:
        return cls._registry

    @classmethod
    def get_all_registered_target_layers(cls) -> Dict[str, ETLLayer]:
        return cls._target_layer_map

    @classmethod
    def get_all_registered_domain_sources(cls) -> Dict[str, DomainSource]: # <-- NOWA METODA
        return cls._domain_source_map