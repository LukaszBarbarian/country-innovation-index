import os, sys
import sys
sys.path.insert(0, "d:/projects/cv-demo1")

print("sys.path:", sys.path)


from src.silver.orchestrators.silver_orchestrator import SilverOrchestrator
from src.silver.contexts.layer_runtime_context import LayerRuntimeContext
from src.silver.contexts.silver_payload_parser import SilverPayloadParser
from src.common.config.config_manager import ConfigManager


import json

# Ciąg znaków zawierający dane JSON
json_string = """
{
    "status": "COMPLETED",
    "correlation_id": "1",
    "queue_message_id": "372c2e90-c4f0-4391-a674-27129fa0f7a6",
    "api_name": "NOBELPRIZE",
    "dataset_name": "nobelPrizes",
    "layer_name": "Bronze",
    "message": "API data successfully processed and stored to Bronze.",
    "api_response_status_code": null,
    "output_path": "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/NOBELPRIZE/2025/08/01/nobelPrizes_1_c237f2d9.json"
}
"""


payload = json.loads(json_string)





payload_parser = SilverPayloadParser()
context = payload_parser.parse(payload)

# runtime_context = LayerRuntimeContext(spark=spark, layer_context=context)
# config = ConfigManager()

# orchestrator = SilverOrchestrator(config)

# result = await orchestrator.run(runtime_context)