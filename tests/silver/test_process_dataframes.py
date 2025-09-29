import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from src.common.enums.env import Env
from src.common.enums.etl_layers import ETLLayer
from src.silver.builders.model_director import ModelDirector
from src.silver.models.models import SilverManifestModel
from src.silver.context.silver_context import SilverContext
from src.common.enums.domain_source import DomainSource

@pytest.mark.asyncio
async def test_process_dataframes():
    mock_context = SilverContext(env=Env.DEV, etl_layer=ETLLayer.SILVER, manifest=MagicMock())
    director = ModelDirector(injector=MagicMock(), context=mock_context)

    mock_df = MagicMock()
    mock_transformer = AsyncMock()
    mock_transformer.transform.return_value = mock_df
    mock_transformer.normalize.return_value = mock_df
    mock_transformer.enrich.return_value = mock_df

    raw_data = {
        DomainSource.NOBELPRIZE: {"laureates": mock_df}
    }

    with patch("src.common.factories.domain_transformer_factory.DomainTransformerFactory.get_instance", return_value=mock_transformer):
        processed = await director._process_dataframes(raw_data)
        key = (DomainSource.NOBELPRIZE, "laureates")
        assert key in processed
        assert processed[key] == mock_df
        mock_transformer.transform.assert_awaited_once()
        mock_transformer.normalize.assert_awaited_once()
        mock_transformer.enrich.assert_awaited_once()
