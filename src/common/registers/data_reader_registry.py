# src/common/registers/data_reader_registry.py
from typing import Dict, Type
from src.common.registers.base_registry import BaseRegistry
from src.common.enums.domain_source import DomainSource
from src.common.readers.base_data_reader import BaseDataReader

class DataReaderRegistry(BaseRegistry[DomainSource, Type[BaseDataReader]]):
    """
    Rejestr dla klas DataReaderów.
    Mapuje DomainSource na POJEDYNCZĄ klasę DataReader, która jest domyślnym
    czytnikiem dla danego DomainSource.
    Wykorzystuje ogólny dekorator 'register' z BaseRegistry.
    """
    _registry: Dict[DomainSource, Type[BaseDataReader]] = {}

    @classmethod
    def _get_registry_dict(cls) -> Dict[DomainSource, Type[BaseDataReader]]:
        """
        Zwraca unikalny słownik rejestru dla DataReaderRegistry.
        Implementacja abstrakcyjnej metody z BaseRegistry.
        """
        return cls._registry
