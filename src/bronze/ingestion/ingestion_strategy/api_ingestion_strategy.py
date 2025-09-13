# src/bronze/ingestion/ingestion_strategy/api_ingestion_strategy.py

from typing import List, Optional, Dict, Any, cast
import logging
import traceback
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.models.manifest import BronzeManifestSource
from src.common.config.config_manager import ConfigManager
from src.common.enums.domain_source_type import DomainSourceType
from src.common.enums.etl_layers import ETLLayer
from src.common.factories.api_client_factory import ApiClientFactory
from src.common.factories.data_processor_factory import DataProcessorFactory
from src.common.factories.storage_file_builder_factory import StorageFileBuilderFactory
from src.bronze.ingestion.ingestion_strategy.base_ingestion_strategy import BaseIngestionStrategy
from src.common.models.file_info import FileInfo
from src.common.models.ingestion_result import IngestionResult
from src.common.models.processed_result import ProcessedResult
from src.common.models.raw_data import RawData
from src.common.registers.ingestion_strategy_registry import IngestionStrategyRegistry
from src.common.azure_clients.blob_client_manager import BlobClientManager
from src.common.utils.decorator_duration import track_duration

logger = logging.getLogger(__name__)

@IngestionStrategyRegistry.register(DomainSourceType.API)
class ApiIngestionStrategy(BaseIngestionStrategy):
    """
    An ingestion strategy specifically designed for fetching data from an API.

    This class orchestrates the end-to-end process of data ingestion from an API source,
    including fetching, processing, and storing the data in a data lake. It is registered
    to handle sources with a `DomainSourceType` of `API`.
    """
    @track_duration
    async def ingest(self, manifest_source: BronzeManifestSource) -> IngestionResult:
        """
        The main method for orchestrating the API data ingestion workflow.

        It performs the following steps:
        1. Retrieves the appropriate `ApiClient`, `DataProcessor`, and `StorageFileBuilder`
           instances from their respective factories based on the manifest source.
        2. Fetches the raw data from the API using the `ApiClient`.
        3. If data is fetched, it processes the raw data using the `DataProcessor`.
        4. Builds a file from the processed data using the `StorageFileBuilder`.
        5. Uploads the file to the Azure Blob Storage using the `BlobClientManager`.
        6. Returns an `IngestionResult` object indicating the status, records processed,
           and file paths. It handles exceptions and returns a `FAILED` status
           if any step fails.

        Args:
            manifest_source (BronzeManifestSource): The specific manifest source
                                                    configuration for the API to be ingested.

        Returns:
            IngestionResult: An object summarizing the outcome of the ingestion process.
        """
        context: BronzeContext = cast(BronzeContext, self.context)
        
        try:
            self.storage_account_name = self.config.get("DATA_LAKE_STORAGE_ACCOUNT_NAME")
            api_client = ApiClientFactory.get_instance(manifest_source.source_config_payload.domain_source, config=self.config)
            data_processor = DataProcessorFactory.get_instance(manifest_source.source_config_payload.domain_source)
            file_builder = StorageFileBuilderFactory.get_instance(ETLLayer.BRONZE, config=self.config)
            storage_manager = BlobClientManager(self.context.etl_layer.value)

            # 1. Fetch data from API
            fetched_data: List[RawData] = await api_client.fetch_all(manifest_source)

            if not fetched_data:
                return self._create_success_result(
                    context, manifest_source, "No records fetched, skipping file save."
                )

            # 2. Process data
            all_processed_results: List[ProcessedResult] = [
                data_processor.process(raw_data.data, self.context) for raw_data in fetched_data
            ]

            # 3. Write to file
            file_output = file_builder.build_file(
                correlation_id=context.correlation_id,
                container_name=context.etl_layer.value,
                storage_account_name=self.storage_account_name,
                processed_records_results=all_processed_results,
                config_payload=manifest_source.source_config_payload,
                ingestion_date=context.ingestion_date
            )

            file_content_bytes = file_output["file_content_bytes"]
            file_info: FileInfo = file_output["file_info"]

            file_size_bytes = await storage_manager.upload_blob(
                file_content_bytes=file_content_bytes,
                file_info=file_info
            )
            
            if not file_size_bytes:
                return self._create_success_result(
                    context, manifest_source, "No data uploaded. File size 0 bytes.", status="SKIPPED"
                )

            return self._create_success_result(
                context=context,
                source=manifest_source,
                message=f"API data successfully processed. Uploaded 1 file with {len(all_processed_results)} records.",
                output_paths=[file_info.full_blob_url],
                records=len(all_processed_results)
            )

        except Exception as e:
            logger.error(f"Error ingesting {manifest_source.source_config_payload.dataset_name}: {e}")
            error_details = {
                "errorType": type(e).__name__,
                "errorMessage": str(e),
                "stackTrace": traceback.format_exc()
            }
            return self._create_error_result(context, manifest_source, e, error_details)

    def _create_success_result(
        self,
        context: BronzeContext,
        source: BronzeManifestSource,
        message: str,
        output_paths: Optional[List[str]] = None,
        status: str = "COMPLETED",
        records: int = 0
    ) -> IngestionResult:
        """A helper method for creating a successful ingestion result object."""
        return IngestionResult(
            correlation_id=context.correlation_id,
            domain_source=source.source_config_payload.domain_source,
            domain_source_type=source.source_config_payload.domain_source_type,
            dataset_name=source.source_config_payload.dataset_name,
            status=status,
            message=message,
            output_paths=output_paths or [],
            duration_in_ms=0,
            record_count=records
        )

    def _create_error_result(
        self,
        context: BronzeContext,
        source: BronzeManifestSource,
        error: Exception,
        error_details: Optional[Dict[str, Any]] = None
    ) -> IngestionResult:
        """A helper method for creating a failed ingestion result object."""
        return IngestionResult(
            correlation_id=context.correlation_id,
            domain_source=source.source_config_payload.domain_source,
            domain_source_type=source.source_config_payload.domain_source_type,
            dataset_name=source.source_config_payload.dataset_name,
            status="FAILED",
            message=f"Ingestion failed: {error}",
            output_paths=[],
            error_details=error_details or {},
            duration_in_ms=0
        )