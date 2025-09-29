from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from src.common.models.orchestrator_result import OrchestratorResult
from src.silver.context.silver_context import SilverContext
from src.silver.models.models import SilverManifestModel
from src.silver.orchestrator.silver_orchestrator import SilverOrchestrator


@pytest.mark.asyncio
async def test_silver_orchestrator_execute_failure():
    models = [SilverManifestModel(model_name="MODEL_FAIL", source_datasets=[])]
    context = SilverContext(env="dev", etl_layer="silver", manifest=MagicMock())
    context.manifest.models = models
    context.correlation_id = "test-corr-id"

    orchestrator = SilverOrchestrator(config=MagicMock())

    # ModelDirector rzuca wyjątek
    mock_director = AsyncMock()
    mock_director.get_built_model.side_effect = RuntimeError("Build failed")

    mock_persister = MagicMock()

    with patch("src.silver.orchestrator.silver_orchestrator.SilverOrchestrator.init_di", return_value=MagicMock(get=MagicMock(return_value=mock_director))):
        with patch("src.silver.orchestrator.silver_orchestrator.ModelPersister", return_value=mock_persister):
            result: OrchestratorResult = await orchestrator.execute(context)

            assert isinstance(result, OrchestratorResult)
            # Status końcowy FAILED
            assert result.status == "FAILED"
            assert "Build failed" in result.message
