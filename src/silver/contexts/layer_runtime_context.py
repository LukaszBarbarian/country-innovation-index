# src/common/context/layer_runtime_context.py

from pyspark.sql import SparkSession
from src.common.contexts.layer_context import LayerContext
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env
from typing import TypeVar, Generic, Any, Dict

LayerContextType = TypeVar("LayerContextType", bound=LayerContext)

class LayerRuntimeContext(Generic[LayerContextType]):
    """
    Kontekst środowiska uruchomieniowego dla danej warstwy ETL.
    Łączy SparkSession z metadanami kontekstu specyficznymi dla warstwy.
    Umożliwia bezpośredni dostęp do kluczowych metadanych z kontekstu warstwy.
    """
    def __init__(self, spark: SparkSession, layer_context: LayerContextType):
        if not isinstance(spark, SparkSession):
            raise TypeError("SparkSession must be provided to LayerRuntimeContext.")
        if not isinstance(layer_context, LayerContext):
            raise TypeError("Layer context must be an instance of LayerContext.")
        
        self._spark = spark
        self._layer_context = layer_context

    @property
    def spark(self) -> SparkSession:
        """
        Zwraca aktywną sesję Sparka.
        """
        return self._spark

    @property
    def layer_context(self) -> LayerContextType:
        """
        Zwraca obiekt kontekstu specyficznego dla warstwy (np. BronzeContext, SilverContext).
        """
        return self._layer_context

    @property
    def correlation_id(self) -> str:
        """
        Zwraca ID korelacji z kontekstu warstwy.
        """
        return self._layer_context.correlation_id

    @property
    def queue_message_id(self) -> str:
        """
        Zwraca ID wiadomości z kolejki z kontekstu warstwy.
        """
        return self._layer_context.queue_message_id

    @property
    def etl_layer(self) -> ETLLayer:
        """
        Zwraca typ warstwy ETL z kontekstu warstwy.
        """
        return self._layer_context.etl_layer

    @property
    def processing_config_payload(self) -> Dict[str, Any]:
        """
        Zwraca payload konfiguracyjny przetwarzania z kontekstu warstwy.
        """
        return self._layer_context.processing_config_payload
    

    @property
    def env(self) -> Env:
        return self._layer_context.env