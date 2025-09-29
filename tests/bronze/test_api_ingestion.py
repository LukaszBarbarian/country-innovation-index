import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.ingestion.ingestion_strategy.api_ingestion_strategy import ApiIngestionStrategy
from src.bronze.models.manifest import BronzeManifestSource
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.common.models.file_info import FileInfo

mock_source = BronzeManifestSource(
    source_config_payload=MagicMock(
        domain_source_type="api",
        domain_source="NOBELPRIZE",
        dataset_name="laureates",
        request_payload={"offset": 0, "limit": 100}
    )
)

@pytest.mark.asyncio
async def test_api_ingestion_success():
    context = BronzeContext(env=Env.DEV, etl_layer=ETLLayer.BRONZE)
    
    strategy = ApiIngestionStrategy(config=MagicMock(), context=context)
    strategy.context = context

    mock_client = AsyncMock()
    mock_client.fetch_all = AsyncMock(return_value=[MagicMock(data={"foo": "bar"})])

    mock_processor = MagicMock()
    mock_processor.process.return_value = {"processed": True}

    mock_file_builder = MagicMock()
    mock_file_builder.build_file.return_value = {
        "file_content_bytes": b"data",
        "file_info": MagicMock(full_blob_url="https://fake.blob/path/file.json")
    }

    mock_blob_manager = AsyncMock()
    mock_blob_manager.upload_blob.return_value = 123

    with patch("src.common.factories.api_client_factory.ApiClientFactory.get_instance", return_value=mock_client), \
         patch("src.common.factories.data_processor_factory.DataProcessorFactory.get_instance", return_value=mock_processor), \
         patch("src.common.factories.storage_file_builder_factory.StorageFileBuilderFactory.get_instance", return_value=mock_file_builder), \
         patch("src.bronze.ingestion.ingestion_strategy.api_ingestion_strategy.BlobClientManager", return_value=mock_blob_manager):

        result = await strategy.ingest(mock_source)

        assert result.status == "COMPLETED"
        assert result.output_paths == ["https://fake.blob/path/file.json"]
        assert result.record_count == 1
        mock_file_builder.build_file.assert_called_once()
        mock_blob_manager.upload_blob.assert_awaited_once()

