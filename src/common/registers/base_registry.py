# src/common/registry/base_registry.py

from typing import Dict, Type, TypeVar, Generic, Any
from abc import ABC, abstractmethod

T = TypeVar("T")      # Registered class type
K = TypeVar("K")      # Registration key type (e.g., enum, str)

class BaseRegistry(Generic[K, T], ABC):
    """
    An abstract base class for implementing the Registry pattern.
    
    This class provides a generic mechanism to register and retrieve classes
    using a unique key. It forces subclasses to define their own registry
    dictionary, preventing name collisions between different registries.
    """

    @classmethod
    @abstractmethod
    def _get_registry_dict(cls) -> Dict[K, Type[T]]:
        """
        An abstract class method that returns the unique registry dictionary for
        the specific subclass. Subclasses must implement this to define where
        their registered classes are stored.
        """
        pass

    @classmethod
    def register(cls, key: K):
        """
        A decorator used to register a class with the registry.
        
        Args:
            key (K): The unique key to associate with the class.

        Raises:
            ValueError: If the key is already registered.
        """
        def decorator(target_cls: Type[T]):
            registry_dict = cls._get_registry_dict()
            if key in registry_dict:
                raise ValueError(f"{cls.__name__}: Key '{key}' is already registered with {registry_dict[key].__name__}")
            registry_dict[key] = target_cls
            return target_cls
        return decorator

    @classmethod
    def get_class(cls, key: K) -> Type[T]:
        """
        Retrieves a registered class based on its key.
        
        Args:
            key (K): The key used to look up the class.

        Returns:
            Type[T]: The registered class.

        Raises:
            ValueError: If no class is found for the given key.
        """
        registry_dict = cls._get_registry_dict()
        if key not in registry_dict:
            raise ValueError(f"{cls.__name__}: Key '{key}' - Unknown source")
        return registry_dict[key]