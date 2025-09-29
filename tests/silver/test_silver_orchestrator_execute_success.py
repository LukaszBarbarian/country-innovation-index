import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import pytest_asyncio
from src.common.spark.spark_service import SparkService
from src.silver.orchestrator.silver_orchestrator import SilverOrchestrator
from src.silver.context.silver_context import SilverContext
from src.silver.models.models import SilverManifestModel
from src.common.models.base_process_result import BaseProcessResult
from src.common.models.orchestrator_result import OrchestratorResult


@pytest_asyncio.fixture(scope="session")
async def spark_service():
    service = SparkService()
    service.start_local(app_name="silver_test")
    yield service
    service.stop()



@pytest.mark.asyncio
async def test_silver_orchestrator_execute_success():
    # Przygotowanie manifestu
    models = [SilverManifestModel(model_name=f"MODEL_{i}", source_datasets=[], table_name="TAB") for i in range(3)]
    context = SilverContext(env="dev", etl_layer="silver", manifest=MagicMock())
    context.manifest.models = models
    context.correlation_id = "test-corr-id"

    orchestrator = SilverOrchestrator(config=MagicMock(), spark=spark_service)

    # Mockowanie ModelDirector i Persister
    mock_director = AsyncMock()
    mock_persister = MagicMock()
    mock_result = MagicMock(spec=BaseProcessResult)
    mock_persister.persist_model.return_value = mock_result
    mock_director.get_built_model.return_value = MagicMock()

    with patch("src.silver.orchestrator.silver_orchestrator.SilverOrchestrator.init_di", return_value=MagicMock(get=MagicMock(return_value=mock_director))):
        with patch("src.silver.orchestrator.silver_orchestrator.ModelPersister", return_value=mock_persister):
            result: OrchestratorResult = await orchestrator.execute(context)
            
            assert isinstance(result, OrchestratorResult)
            # Wszystkie modele powinny być przetworzone
            assert len(result.results) == len(models)
            for r in result.results:
                assert r.status == mock_result.status
