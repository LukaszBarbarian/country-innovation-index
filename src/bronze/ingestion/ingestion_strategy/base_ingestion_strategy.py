# src/common/ingestion/base_ingestion_strategy.py

from abc import ABC, abstractmethod
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.models.manifest import BronzeManifestSource
from src.common.config.config_manager import ConfigManager
from src.common.models.base_context import ContextBase
from src.common.models.ingestion_result import IngestionResult

class BaseIngestionStrategy(ABC):
    """
    An abstract base class for all data ingestion strategies.
    This class defines the common interface that concrete strategies must implement,
    ensuring a standardized approach to handling different data source types.
    """
    def __init__(self, config: ConfigManager, context: ContextBase):
        """
        Initializes the ingestion strategy with a configuration manager and a context object.

        Args:
            config (ConfigManager): An instance of the configuration manager to access settings.
            context (ContextBase): The context object for the current ingestion process,
                                   containing metadata and state information.
        """
        self.config = config
        self.context = context

    @abstractmethod
    async def ingest(self, source: BronzeManifestSource) -> IngestionResult:
        """
        An abstract method to perform the data ingestion operation.

        This method must be implemented by all subclasses. It orchestrates the process
        of fetching, processing, and storing data from a specific source, and is
        expected to return the result of the operation.

        Args:
            source (BronzeManifestSource): A manifest object containing the configuration
                                           details for the specific data source.

        Returns:
            IngestionResult: An object summarizing the outcome of the ingestion process.
        """
        pass