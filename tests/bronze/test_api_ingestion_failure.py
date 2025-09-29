import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.bronze.contexts.bronze_context import BronzeContext
from src.bronze.ingestion.ingestion_strategy.api_ingestion_strategy import ApiIngestionStrategy
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.bronze.models.manifest import BronzeManifestSource
from src.common.models.file_info import FileInfo

# przykładowy manifest source
mock_source = BronzeManifestSource(
    source_config_payload=MagicMock(
        domain_source="NOBELPRIZE",
        domain_source_type="api",
        dataset_name="laureates"
    )
)

@pytest.mark.asyncio
async def test_ingest_failure_api_client_factory():
    """Exception in the API client factory should result in FAILED ingestion status"""
    context = BronzeContext(env=Env.DEV, etl_layer=ETLLayer.BRONZE)
    strategy = ApiIngestionStrategy(config=MagicMock(), context=context)

    with patch("src.common.factories.api_client_factory.ApiClientFactory.get_instance", side_effect=Exception("Factory error")):
        result = await strategy.ingest(mock_source)
        assert result.status == "FAILED"
        assert "Factory error" in result.message
        assert result.output_paths == []

@pytest.mark.asyncio
async def test_ingest_failure_fetch_all_complete():
    """All dependencies mocked, simulate API fetch_all raising exception"""
    
    # Przygotowanie kontekstu i strategii
    context = BronzeContext(env=Env.DEV, etl_layer=ETLLayer.BRONZE)
    strategy = ApiIngestionStrategy(config=MagicMock(), context=context)
    
    # Mock dla API klienta
    mock_client = AsyncMock()
    mock_client.fetch_all.side_effect = Exception("API error")
    
    # Mock dla pozostałych fabryk
    mock_processor = MagicMock()
    mock_file_builder = MagicMock()
    mock_file_builder.build_file.return_value = {
        "file_content_bytes": b"dummy",
        "file_info": MagicMock(full_blob_url="https://fake.blob/path/file.json")
    }
    mock_blob_manager = AsyncMock()
    mock_blob_manager.upload_blob.return_value = 1024  # 1 KB
    
    # Mock config.get
    strategy.config.get.return_value = "fake_account"
    
    # Przygotowanie przykładowego manifest_source
    class MockSource:
        source_config_payload = type("Payload", (), {
            "domain_source": "NOBELPRIZE",
            "domain_source_type": "api",
            "dataset_name": "laureates"
        })()
    mock_source = MockSource()
    
    with patch("src.common.factories.api_client_factory.ApiClientFactory.get_instance", return_value=mock_client), \
         patch("src.common.factories.data_processor_factory.DataProcessorFactory.get_instance", return_value=mock_processor), \
         patch("src.common.factories.storage_file_builder_factory.StorageFileBuilderFactory.get_instance", return_value=mock_file_builder), \
         patch("src.bronze.ingestion.ingestion_strategy.api_ingestion_strategy.BlobClientManager", return_value=mock_blob_manager):
        
        result = await strategy.ingest(mock_source)
        
        # Sprawdzenie, że strategia zgłosiła FAILED status
        assert result.status == "FAILED"
        assert "API error" in result.message

@pytest.mark.asyncio
async def test_ingest_failure_data_processor_factory():
    """Exception in the DataProcessor factory should result in FAILED ingestion status"""
    context = BronzeContext(env=Env.DEV, etl_layer=ETLLayer.BRONZE)
    strategy = ApiIngestionStrategy(config=MagicMock(), context=context)
    mock_client = AsyncMock()
    mock_client.fetch_all.return_value = [MagicMock(data={"some": "data"})]

    with patch("src.common.factories.api_client_factory.ApiClientFactory.get_instance", return_value=mock_client), \
         patch("src.common.factories.data_processor_factory.DataProcessorFactory.get_instance", side_effect=Exception("Processor factory error")):
        result = await strategy.ingest(mock_source)
        assert result.status == "FAILED"
        assert "Processor factory error" in result.message

@pytest.mark.asyncio
async def test_ingest_failure_file_builder():
    """Exception raised during file building should result in FAILED ingestion status"""
    context = BronzeContext(env=Env.DEV, etl_layer=ETLLayer.BRONZE)
    strategy = ApiIngestionStrategy(config=MagicMock(), context=context)
    mock_client = AsyncMock()
    mock_client.fetch_all.return_value = [MagicMock(data={"some": "data"})]
    mock_processor = MagicMock()
    mock_processor.process.return_value = MagicMock()

    with patch("src.common.factories.api_client_factory.ApiClientFactory.get_instance", return_value=mock_client), \
         patch("src.common.factories.data_processor_factory.DataProcessorFactory.get_instance", return_value=mock_processor), \
         patch("src.common.factories.storage_file_builder_factory.StorageFileBuilderFactory.get_instance", side_effect=Exception("File builder error")):
        result = await strategy.ingest(mock_source)
        assert result.status == "FAILED"
        assert "File builder error" in result.message

@pytest.mark.asyncio
async def test_ingest_failure_blob_upload():
    """Exception raised during blob upload should result in FAILED ingestion status"""
    context = BronzeContext(env=Env.DEV, etl_layer=ETLLayer.BRONZE)
    strategy = ApiIngestionStrategy(config=MagicMock(), context=context)
    mock_client = AsyncMock()
    mock_client.fetch_all.return_value = [MagicMock(data={"some": "data"})]
    mock_processor = MagicMock()
    mock_processor.process.return_value = MagicMock()
    mock_file_builder = MagicMock()
    mock_file_builder.build_file.return_value = {
        "file_content_bytes": b"data",
        "file_info": MagicMock(full_blob_url="https://fake.blob/path/file.json")
    }
    mock_blob_manager = AsyncMock()
    mock_blob_manager.upload_blob.side_effect = Exception("Blob upload error")

    with patch("src.common.factories.api_client_factory.ApiClientFactory.get_instance", return_value=mock_client), \
         patch("src.common.factories.data_processor_factory.DataProcessorFactory.get_instance", return_value=mock_processor), \
         patch("src.common.factories.storage_file_builder_factory.StorageFileBuilderFactory.get_instance", return_value=mock_file_builder), \
         patch("src.bronze.ingestion.ingestion_strategy.api_ingestion_strategy.BlobClientManager", return_value=mock_blob_manager):
        result = await strategy.ingest(mock_source)
        assert result.status == "FAILED"
        assert "Blob upload error" in result.message
