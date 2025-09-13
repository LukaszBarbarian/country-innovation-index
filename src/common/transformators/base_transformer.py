from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseTransformer(ABC):
    """
    An abstract base class for data transformers within a specific DomainSource.

    This class defines a contract for transforming, normalizing, and enriching
    data at different stages of the ETL process. All methods are asynchronous
    to support non-blocking operations.
    """

    @abstractmethod
    async def transform(self, df: DataFrame, dataset_name: str = "") -> DataFrame:
        """
        The transformation stage (column selection, initial processing).

        This method should be implemented to perform initial, high-level
        transformations on the input DataFrame, such as selecting necessary
        columns or renaming them.

        Args:
            df (DataFrame): The input Spark DataFrame to transform.
            dataset_name (str): The name of the dataset being processed.

        Returns:
            DataFrame: The transformed Spark DataFrame.
        """
        raise NotImplementedError

    @abstractmethod
    async def normalize(self, df: DataFrame, dataset_name: str = "") -> DataFrame:
        """
        The normalization stage (standardizing format, creating keys).

        This method should be implemented to ensure data consistency,
        unify data formats, and create any necessary keys for joining or
        uniquely identifying records.

        Args:
            df (DataFrame): The input Spark DataFrame to normalize.
            dataset_name (str): The name of the dataset being processed.

        Returns:
            DataFrame: The normalized Spark DataFrame.
        """
        raise NotImplementedError

    @abstractmethod
    async def enrich(self, df: DataFrame, dataset_name: str = "") -> DataFrame:
        """
        The data enrichment stage (optional).

        This method should be implemented to add new information to the
        DataFrame, such as by joining with other data sources or calculating
        new fields.

        Args:
            df (DataFrame): The input Spark DataFrame to enrich.
            dataset_name (str): The name of the dataset being processed.

        Returns:
            DataFrame: The enriched Spark DataFrame.
        """
        raise NotImplementedError