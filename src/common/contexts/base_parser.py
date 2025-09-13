

from typing import Optional
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase


class BaseParser():
    """
    An abstract base class for parsers in the data processing pipeline.
    This class defines the foundational structure and methods that all concrete
    parser classes must implement. It ensures a standardized approach for
    handling manifest and context data.
    """
    def __init__(self, config: ConfigManager):
        """
        Initializes the BaseParser with a configuration manager.

        Args:
            config (ConfigManager): An instance of the ConfigManager, used to access
                                    application and data-related settings.
        """
        self.config = config

    def parse(self, manifest_json: str, summary_json: Optional[str] = None) -> ContextBase:
        """
        Parses a JSON manifest and an optional JSON summary to create a context object.

        This method is a template and must be overridden by any subclass. It enforces
        the common interface for all parsers, ensuring they have a `parse` method
        that accepts manifest and summary JSON strings and returns a context object.

        Args:
            manifest_json (str): A string containing the JSON representation of the manifest.
            summary_json (Optional[str]): An optional string with the JSON summary.

        Raises:
            NotImplementedError: This exception is raised to indicate that the method
                                 must be implemented in any concrete subclass.
        """
        raise NotImplementedError("Must implement parse in subclass")