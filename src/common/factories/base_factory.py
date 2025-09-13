# src/functions/common/factory/base_factory_from_registry.py

from typing import TypeVar, Generic, Any, Type
from abc import ABC, abstractmethod

K = TypeVar("K")  # Key type
T = TypeVar("T")  # Returned instance type

class BaseFactoryFromRegistry(Generic[K, T], ABC):
    """
    A base class for factories that use a registry to retrieve classes.
    This class implements the Factory pattern, centralizing the logic for
    creating objects based on a given key.
    """

    @classmethod
    @abstractmethod
    def get_registry(cls):
        """
        An abstract class method that returns the registry object.
        Subclasses must implement this to provide a specific registry.

        The registry can be a dictionary or any object with a 'get_class' or 'get' method.
        """
        raise NotImplementedError

    @classmethod
    def get_class(cls, key: K) -> Type[T]:
        """
        Retrieves the class corresponding to a given key from the registry.

        Args:
            key (K): The key used to look up the class in the registry.

        Returns:
            Type[T]: The class associated with the key.
        """
        registry = cls.get_registry()
        get_class = registry.get_class if hasattr(registry, "get_class") else registry.get
        klass = get_class(key)
        # if klass is None:
        #     raise ValueError(f"No class registered for key: {key}")
        return klass

    @classmethod
    def get_instance(cls, key: K, *args: Any, **kwargs: Any) -> T:
        """
        Creates an instance of the class corresponding to the key.

        This method first retrieves the class using `get_class` and then
        instantiates it, passing any provided arguments.

        Args:
            key (K): The key for the class to be instantiated.
            *args: Positional arguments to pass to the class's constructor.
            **kwargs: Keyword arguments to pass to the class's constructor.

        Returns:
            T: An instance of the requested class.
        """
        klass = cls.get_class(key)
        return klass(*args, **kwargs)