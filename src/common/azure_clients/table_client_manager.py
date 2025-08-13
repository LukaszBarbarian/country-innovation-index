import logging
from typing import Any, Dict, List, Optional, Union, Literal
from azure.data.tables.aio import TableServiceClient, TableClient
from azure.data.tables import UpdateMode
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

# Pamiętaj, że musisz mieć zainstalowane azure-data-tables: pip install azure-data-tables

# Importuj swoją klasę bazową
from src.common.azure_clients.base_azure_client_manager import AzureClientManagerBase

logger = logging.getLogger(__name__)

# Typ dla encji, to po prostu słownik
TableEntity = Dict[str, Any]

class TableStorageManager(AzureClientManagerBase[TableServiceClient, TableClient]):
    """
    Menedżer do interakcji z Azure Table Storage dla określonej tabeli.
    Wykorzystuje AzureClientManagerBase do zarządzania połączeniem.
    Zawiera metody do zarządzania encjami w tabeli.
    """
    def __init__(self, 
                 table_name: str, 
                 storage_account_name_setting_name: str = "DATA_LAKE_STORAGE_ACCOUNT_NAME"):
        """
        Inicjalizuje menedżera Table Storage.

        Args:
            table_name (str): Nazwa tabeli Table Storage.
            storage_account_name_setting_name (str): Nazwa ustawienia connection string w ConfigManagerze.
                                                      Domyślnie "AzureWebJobsStorage".
        """
        super().__init__(
            resource_name=table_name, # resource_name to teraz nazwa tabeli
            storage_account_name_setting_name=storage_account_name_setting_name,
            base_url_suffix=".table.core.windows.net" # Sufiks dla Table Storage
        )
        logger.info(f"TableStorageManager initialized for table '{self.resource_name}'.")

    # Implementacja abstrakcyjnych metod z AzureClientManagerBase
    def _create_service_client_from_identity(self, account_url: str, credential) -> TableServiceClient:
        """Tworzy TableServiceClient używając tożsamości."""
        return TableServiceClient(endpoint=account_url, credential=credential)

    def _get_resource_client(self, service_client: TableServiceClient, table_name: str) -> TableClient:
        """Pobiera TableClient dla określonej tabeli."""
        return service_client.get_table_client(table_name)

    async def create_table_if_not_exists(self):
        """Tworzy tabelę, jeśli nie istnieje."""
        try:
            await self.client.create_table()
            logger.info(f"Table '{self.resource_name}' created successfully.")
        except ResourceExistsError:
            logger.debug(f"Table '{self.resource_name}' already exists. Skipping creation.")
        except Exception as e:
            logger.error(f"Error creating table '{self.resource_name}': {e}", exc_info=True)
            raise

    async def upsert_entity(self, entity: TableEntity, mode: Literal['merge', 'replace'] = 'merge') -> TableEntity:
        """
        Wstawia lub aktualizuje encję w tabeli.
        Wymaga 'PartitionKey' i 'RowKey' w encji.
        """
        await self.create_table_if_not_exists() # Zapewnij istnienie tabeli przed operacją
        try:
            if mode == 'merge':
                await self.client.upsert_entity(entity=entity, mode=UpdateMode.MERGE)
            elif mode == 'replace':
                await self.client.upsert_entity(entity=entity, mode=UpdateMode.REPLACE)
            else:
                raise ValueError("Mode must be 'merge' or 'replace'.")
            logger.info(f"Entity with PartitionKey='{entity.get('PartitionKey')}' and RowKey='{entity.get('RowKey')}' upserted successfully.")
            return entity
        except Exception as e:
            logger.error(f"Error upserting entity with PartitionKey='{entity.get('PartitionKey')}' and RowKey='{entity.get('RowKey')}': {e}", exc_info=True)
            raise

    async def get_entity(self, partition_key: str, row_key: str) -> Optional[TableEntity]:
        """
        Pobiera pojedynczą encję z tabeli.
        """
        await self.create_table_if_not_exists() # Zapewnij istnienie tabeli
        try:
            entity = await self.client.get_entity(partition_key=partition_key, row_key=row_key)
            logger.info(f"Entity with PartitionKey='{partition_key}' and RowKey='{row_key}' retrieved successfully.")
            return entity
        except ResourceNotFoundError:
            logger.warning(f"Entity with PartitionKey='{partition_key}' and RowKey='{row_key}' not found.")
            return None
        except Exception as e:
            logger.error(f"Error getting entity with PartitionKey='{partition_key}' and RowKey='{row_key}': {e}", exc_info=True)
            raise

    async def query_entities(self, filter_query: Optional[str] = None, select: Optional[List[str]] = None) -> List[TableEntity]:
        """
        Wykonuje zapytanie do tabeli i zwraca listę pasujących encji.
        Przykład filter_query: "PartitionKey eq 'batch123' and Status eq 'PENDING'"
        """
        await self.create_table_if_not_exists() # Zapewnij istnienie tabeli
        try:
            entities = []
            async for entity in self.client.query_entities(filter=filter_query, select=select):
                entities.append(entity)
            logger.info(f"Query returned {len(entities)} entities with filter: '{filter_query or 'None'}'.")
            return entities
        except Exception as e:
            logger.error(f"Error querying entities with filter '{filter_query}': {e}", exc_info=True)
            raise

    async def delete_entity(self, partition_key: str, row_key: str):
        """
        Usuwa pojedynczą encję z tabeli.
        """
        await self.create_table_if_not_exists()
        try:
            await self.client.delete_entity(partition_key=partition_key, row_key=row_key)
            logger.info(f"Entity with PartitionKey='{partition_key}' and RowKey='{row_key}' deleted successfully.")
        except ResourceNotFoundError:
            logger.warning(f"Attempted to delete non-existent entity with PartitionKey='{partition_key}' and RowKey='{row_key}'.")
        except Exception as e:
            logger.error(f"Error deleting entity with PartitionKey='{partition_key}' and RowKey='{row_key}': {e}", exc_info=True)
            raise

    async def __aenter__(self):
        await self.create_table_if_not_exists()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.close()