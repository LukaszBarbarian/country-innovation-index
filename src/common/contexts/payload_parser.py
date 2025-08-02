from src.common.contexts.base_layer_context import BaseLayerContext
from typing import Dict, Any, Optional
from src.common.enums.etl_layers import ETLLayer
from src.common.enums.env import Env


class PayloadParser():
    def __init__(self):
        pass

    def parse(self, payload: Dict[str, Any]) -> BaseLayerContext:

        required_api_config_fields = ["correlation_id", "queue_message_id", "etl_layer", "env"]
        if not all(field in payload for field in required_api_config_fields):
            missing_fields = [f for f in required_api_config_fields if f not in payload]
            raise ValueError(f"Missing required fields in 'payload': {', '.join(missing_fields)}")
        
        return BaseLayerContext(correlation_id=payload.get("correlation_id"),
                                queue_message_id=payload.get("queue_message_id"),
                                etl_layer=self._map_etl_layer(payload.get("etl_layer")),
                                env=self._map_env(payload.get("env")))
    

    def _map_etl_layer(self, etl_layer: str) -> ETLLayer:
        """
        Maps an layer name string to a ETLLayer enum member.
        Private method for internal use during initialization.
        """
        try:
            etl = ETLLayer(etl_layer)
            return etl
        except ValueError:
            raise ValueError(f"Invalid Layer name: '{etl_layer}'. Expected one of: {[e.value for e in ETLLayer]}")
        except Exception as e:
            raise RuntimeError(f"An unexpected error occurred during etl layer name mapping: {e}")
        

        
    def _map_env(self, env_str: str) -> Env:
        """
        Maps an env name string to a Env enum member.
        Private method for internal use during initialization.
        """
        try:
            env = Env(env_str)
            return env
        except ValueError:
            raise ValueError(f"Invalid Env name: '{env_str}'. Expected one of: {[e.value for e in Env]}")
        except Exception as e:
            raise RuntimeError(f"An unexpected error occurred during Env layer name mapping: {e}")