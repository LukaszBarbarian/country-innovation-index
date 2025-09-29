from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.silver.builders.model_director import ModelDirector
from src.silver.context.silver_context import SilverContext
from src.silver.models.models import SilverManifestModel


@pytest.mark.asyncio
async def test_get_built_model_simple():
    # Kontekst i manifest
    mock_context = SilverContext(env="dev", etl_layer="silver", manifest=MagicMock())
    mock_context._cache = MagicMock()
    mock_context._cache.exists.return_value = False

    model = SilverManifestModel(model_name="COUNTRY", source_datasets=[], depends_on=[])
    director = ModelDirector(injector=MagicMock(), context=mock_context)

    # Mock builder
    mock_builder = AsyncMock()
    mock_builder.load_data.return_value = {}
    mock_builder.build.return_value = MagicMock()
    mock_builder.create_model.return_value = MagicMock()
    mock_builder.synthetic = True

    with patch("src.common.factories.model_builder_factory.ModelBuilderFactory.get_class", return_value=lambda: mock_builder):
        built_model = await director.get_built_model(model)
        assert built_model is not None
