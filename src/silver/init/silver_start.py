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
	"correlationId": "1",
	"queueMessageId": "372c2e90-c4f0-4391-a674-27129fa0f7a6",
	"apiName": "NOBELPRIZE",
	"datasetName": "nobelPrizes",
	"layerName": "Bronze",
	"message": "API data successfully processed and stored to Bronze.",
	"apiResponseStatusCode": null,
	"outputPath": "https://demosurdevdatalake4418sa.blob.core.windows.net/bronze/NOBELPRIZE/2025/08/01/nobelPrizes_1_c237f2d9.json",
	"effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime (North Europe)",
	"executionDuration": 0,
	"durationInQueue": {
		"integrationRuntimeQueue": 0
	},
	"billingReference": {
		"activityType": "ExternalActivity",
		"billableDuration": [
			{
				"meterType": "AzureIR",
				"duration": 0.016666666666666666,
				"unit": "Hours"
			}
		]
	}
}
"""

payload = json.loads(json_string)





payload_parser = SilverPayloadParser()
context = payload_parser.parse(payload)

# runtime_context = LayerRuntimeContext(spark=spark, layer_context=context)
# config = ConfigManager()

# orchestrator = SilverOrchestrator(config)

# result = await orchestrator.run(runtime_context)